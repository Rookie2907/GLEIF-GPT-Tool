# file: app/gleif_unified_ui.py
"""
Unified GLEIF LEI Toolkit — Single lookup + Batch compare

- Single tab: quick LEI lookup.
- Batch tab: Excel-driven scrape of GLEIF results + GPT extraction, now concurrent.
"""
from __future__ import annotations

import os
import re
import sys
import time
import random
import logging
import threading
from functools import lru_cache
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Callable
from urllib.parse import quote_plus

import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent

# Tk
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# -------------------------- Configuration --------------------------
APIKEY = os.environ.get("OPEN_API_KEY")
ORGID = os.environ.get("OPEN_ORG_ID")
client = OpenAI(organization=ORGID, api_key=APIKEY) if ORGID else OpenAI(api_key=APIKEY)

APP_TITLE = "GLEIF LEI Toolkit"
BASE_URL = "https://search.gleif.org/#/search/"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_FILE = os.path.join(SCRIPT_DIR, "ProcessedLEIResults.csv")
LOG_FILE = os.path.join(SCRIPT_DIR, "Processing.log")

# Concurrency + pacing
MAX_FETCH_WORKERS = int(os.getenv("GLEIF_FETCH_WORKERS", "4"))
MAX_GPT_WORKERS = int(os.getenv("GLEIF_GPT_WORKERS", str(MAX_FETCH_WORKERS)))
FETCH_RPS = float(os.getenv("GLEIF_FETCH_RPS", "1.5"))
GPT_RPS = float(os.getenv("GLEIF_GPT_RPS", "2.0"))
DEFAULT_DELAY_RANGE = (1.0, 3.0)
LONG_PAUSE_EVERY = 25
LONG_PAUSE_RANGE = (6.0, 12.0)
RESTART_THRESHOLD = 120
PAGELOAD_TIMEOUT = 55
SCRIPT_TIMEOUT = 35

# Backoff
BACKOFF_START = 0.6
BACKOFF_MAX = 8.0
BACKOFF_MULT = 1.9

# Columns
COLUMNS_TO_INCLUDE = [
    "Internal ID", "Legal Entity Name", "LEI", "Entity Type",
    "Incorporated Country", "Legal Postcode", "Registered Address",
    "Registered Postcode", "Registered City",
]

# Logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logging.getLogger().addHandler(console)

# Preload driver binary
CHROMEDRIVER_PATH = ChromeDriverManager().install()

# -------------------------- Utilities --------------------------
LEI_RE = re.compile(r"\b[0-9A-Z]{18}[0-9]{2}\b")

def random_user_agent() -> str:
    try:
        ua = UserAgent(browsers="Chrome")
        return ua.random
    except Exception:
        pool = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ]
        return random.choice(pool)

# -------------------------- Selenium Helpers --------------------------
def build_chrome_options(headless: bool = True) -> Options:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={random_user_agent()}")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 2,
        "profile.managed_default_content_settings.fonts": 2,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.media_stream": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.plugins": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-sync")
    opts.add_argument("--disable-logging")
    opts.add_argument("--log-level=3")
    opts.add_argument("--metrics-recording-only")
    opts.add_argument("--disable-component-update")
    opts.add_argument("--password-store=basic")
    opts.add_argument("--force-color-profile=srgb")
    opts.add_argument("--disable-features=PushMessaging,AudioServiceOutOfProcess,AutofillServerCommunication,MediaSessionService,OptimizationHints,TranslateUI,NetworkServiceInProcess,InterestCohortAPI,LiveCaption")
    return opts

def get_webdriver(headless: bool = True) -> webdriver.Chrome:
    drv = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=build_chrome_options(headless))
    drv.set_page_load_timeout(PAGELOAD_TIMEOUT)
    drv.set_script_timeout(SCRIPT_TIMEOUT)
    return drv

def count(driver: webdriver.Chrome, sel: str) -> int:
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, sel))
    except Exception:
        return 0

def maybe_accept_cookies(driver: webdriver.Chrome) -> None:
    try:
        for sel in [
            "#onetrust-accept-btn-handler",
            "button#onetrust-accept-btn-handler",
            "button.cookie-accept",
            "button[aria-label*='Accept']",
        ]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                if el.is_displayed():
                    el.click()
                    time.sleep(0.3)
                    break
            except Exception:
                pass
        # JS fallback
        driver.execute_script("""
            const labels = ['accept','agree','allow all','allow'];
            const els = Array.from(document.querySelectorAll('button, [role=button], a'));
            for (const e of els) {
                const t = (e.innerText||'').trim().toLowerCase();
                if (labels.some(x=> t.includes(x))) { try { e.click(); } catch(_){} }
            }
        """)
        time.sleep(0.2)
    except Exception:
        pass

def fetch_page_source_with_selenium(
    url: str,
    driver: webdriver.Chrome,
    wait_css: Optional[str] = None,
    timeout: float = 45.0,
    min_lei: int = 1,
    min_name: int = 1,
    min_country: int = 1,
) -> str:
    def navigate_and_wait(u: str) -> None:
        driver.get(u)
        try:
            driver.delete_all_cookies()
        except Exception:
            pass
        try:
            WebDriverWait(driver, min(25, timeout)).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except Exception:
            logging.warning("Timed out waiting for document.readyState on %s", u)
        maybe_accept_cookies(driver)
        # Strict waits
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: (
                    count(d, ".table-cell.lei") >= min_lei and
                    count(d, ".table-cell.legal-name") >= min_name and
                    count(d, ".table-cell.country") >= min_country
                )
            )
        except Exception:
            logging.warning("Timeout waiting for LEI/name/country on %s", u)
        if wait_css:
            try:
                WebDriverWait(driver, max(3, timeout * 0.3)).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, wait_css))
                )
            except Exception:
                logging.warning("Timeout waiting for '%s' on %s", wait_css, u)
        try:
            for _ in range(3):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.25)
        except Exception:
            pass

    # Navigate with provided URL
    navigate_and_wait(url)
    # If no elements found, retry once with corrected URL form
    lei_ct = count(driver, ".table-cell.lei")
    name_ct = count(driver, ".table-cell.legal-name")
    ctry_ct = count(driver, ".table-cell.country")
    if lei_ct < 1 or name_ct < 1 or ctry_ct < 1:
        try:
            base = BASE_URL  # ensure trailing '/#/search/'
            # if URL mistakenly contains '/#/search/?', rebuild without '?'
            if "/#/search/?" in url:
                fixed = url.replace("/#/search/?", "/#/search/")
                logging.info("Retrying with corrected URL: %s", fixed)
                navigate_and_wait(fixed)
        except Exception:
            pass

    # Log element counts for diagnostics
    logging.info("Element counts on page: lei=%d, name=%d, country=%d", count(driver, ".table-cell.lei"), count(driver, ".table-cell.legal-name"), count(driver, ".table-cell.country"))
    return driver.page_source

# -------------------------- GPT Extraction --------------------------
@lru_cache(maxsize=2000)
def query_gpt_cached(company_name: str, search_results: str) -> str:
    if not search_results:
        return "Not found"
    prompt = (
        "You will be given a company name and the plain text of a GLEIF search results page. "
        "Return ONLY the 20-character LEI for the best UK-incorporated match, or 'Not found'.\n\n"
        f"Company: {company_name}\n---\n{search_results[:120000]}\n---\n"
        "Answer with either the LEI or Not found."
    )
    try:
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": "Be concise. Output only the LEI or Not found."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            timeout=30.0,
        )
        text = (resp.choices[0].message.content or "").strip()
        if text.lower().startswith("not found"):
            return "Not found"
        m = LEI_RE.search(text)
        return m.group(0) if m else "Not found"
    except Exception as e:
        logging.warning("OpenAI error for %s: %s", company_name, e)
        m = LEI_RE.search(search_results)
        return m.group(0) if m else "Error"

def simplify_results_for_gpt(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    return " ".join(soup.get_text(" ").split())[:150000]

def extract_lei_fallback(company_name: str, text: str) -> str:
    """Regex-only fallback if GPT says Not found/Error."""
    if not text:
        return "Not found"
    m = LEI_RE.search(text)
    return m.group(0) if m else "Not found"

# -------------------------- Single Lookup Flow --------------------------
def search_url(company_name: str) -> str:
    # Correct form: .../#/search/simpleSearch=...&perPage=200  (no '?' after /search/)
    return f"{BASE_URL}simpleSearch={quote_plus(company_name)}&perPage=200"

def lookup_company_lei(company_name: str) -> Tuple[str, float]:
    start = time.time()
    driver = get_webdriver(headless=True)
    try:
        url = search_url(company_name)
        page = fetch_page_source_with_selenium(url, driver, wait_css=".table-cell.lei")
        text = simplify_results_for_gpt(page)
        lei = query_gpt_cached(company_name, text)
        if lei in ("Not found", "Error"):
            lei = extract_lei_fallback(company_name, text)
        return lei, time.time() - start
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# -------------------------- Batch (Concurrent) --------------------------
@dataclass
class CompanyResult:
    company_name: str
    source_lei: str
    extracted_lei: str
    match_status: str
    elapsed_sec: float
    search_url: str
    error: Optional[str] = None

def process_data(file_path: str) -> pd.DataFrame:
    # Auto-detect sheet with required columns
    required = set(COLUMNS_TO_INCLUDE)
    xls = pd.ExcelFile(file_path)
    sheet = "SourceData" if "SourceData" in xls.sheet_names else None
    if not sheet:
        for sh in xls.sheet_names:
            hdr = pd.read_excel(file_path, sheet_name=sh, nrows=0)
            if required.issubset(set(map(str, hdr.columns))):
                sheet = sh
                break
    if not sheet:
        raise ValueError(f"No sheet contains required columns. Available: {xls.sheet_names}")
    df = pd.read_excel(file_path, sheet_name=sheet)
    missing = [c for c in COLUMNS_TO_INCLUDE if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing columns: {missing}")
    return df[COLUMNS_TO_INCLUDE].copy()

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = max(0.01, rate_per_sec)
        self.capacity = max(1, burst)
        self.tokens = float(self.capacity)
        self.ts = time.monotonic()
        self.lock = threading.Lock()
    def take(self, tokens: int = 1) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                delta = now - self.ts
                self.ts = now
                self.tokens = min(self.capacity, self.tokens + delta * self.rate)
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
            time.sleep(0.02)

def jitter_sleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))

def backoff_sleep(attempt: int) -> None:
    d = min(BACKOFF_MAX, BACKOFF_START * (BACKOFF_MULT ** max(0, attempt - 1)))
    time.sleep(random.uniform(0, d))

def match_status(source_lei: str, extracted: str) -> str:
    if extracted in ("Not found", "Error", ""):
        return "Not Match"
    return "Match" if (source_lei or "").strip().upper() == extracted.strip().upper() else "Not Match"

def process_one(
    name: str,
    source_lei: str,
    fetch_bucket: TokenBucket,
    gpt_bucket: TokenBucket,
    tlocal: threading.local,
    progress_cb: Optional[Callable[[str], None]],
) -> CompanyResult:
    t0 = time.perf_counter()

    drv = getattr(tlocal, "driver", None)
    made = getattr(tlocal, "made", 0)
    if drv is None or (made and made % RESTART_THRESHOLD == 0):
        try:
            if drv is not None:
                drv.quit()
        except Exception:
            pass
        drv = get_webdriver(headless=True)
        tlocal.driver = drv
        tlocal.made = 0
        if progress_cb:
            progress_cb("[driver] restarted\n")

    tlocal.made += 1
    n = tlocal.made
    jitter_sleep(*DEFAULT_DELAY_RANGE)
    if LONG_PAUSE_EVERY and n % LONG_PAUSE_EVERY == 0:
        jitter_sleep(*LONG_PAUSE_RANGE)

    url = search_url(name)
    html = ""
    for attempt in range(1, 6):
        fetch_bucket.take(1)
        try:
            html = fetch_page_source_with_selenium(url, drv, wait_css=".table-cell.lei")
            if html and len(html) > 1200:
                break
        except Exception as e:
            if progress_cb:
                progress_cb(f"[fetch] {name} attempt {attempt} error: {e}\n")
        backoff_sleep(attempt)

    text = simplify_results_for_gpt(html) if html else ""
    if progress_cb:
        # quick element count diagnostics
        lei_ct = count(drv, ".table-cell.lei")
        name_ct = count(drv, ".table-cell.legal-name")
        ctry_ct = count(drv, ".table-cell.country")
        progress_cb(f"[fetch] {name} ✓ (lei={lei_ct}, name={name_ct}, country={ctry_ct})\n")

    gpt_bucket.take(1)
    try:
        extracted = query_gpt_cached(name, text) if text else "Not found"
    except Exception:
        extracted = "Error"
    if extracted in ("Not found", "Error"):
        fb = extract_lei_fallback(name, text)
        if fb != "Not found":
            extracted = fb

    elapsed = time.perf_counter() - t0
    status = match_status(source_lei, extracted)
    if progress_cb:
        progress_cb(f"[extract] {name} → {extracted} ({status}) in {elapsed:.2f}s\n")

    return CompanyResult(
        company_name=name,
        source_lei=source_lei,
        extracted_lei=extracted,
        match_status=status,
        elapsed_sec=elapsed,
        search_url=url,
        error=None if extracted not in ("Error",) else "OpenAI/regex failure",
    )

def run_batch(
    file_path: str,
    progress_cb: Optional[Callable[[str], None]] = None
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    df = process_data(file_path)
    names: List[str] = df["Legal Entity Name"].astype(str).tolist()
    source_leis: List[str] = [str(x) if pd.notna(x) else "" for x in df["LEI"]]

    if progress_cb:
        progress_cb(f"Loaded {len(names)} companies from Excel. Starting concurrent fetch...\n")

    fetch_bucket = TokenBucket(rate_per_sec=FETCH_RPS, burst=MAX_FETCH_WORKERS)
    gpt_bucket = TokenBucket(rate_per_sec=GPT_RPS, burst=MAX_GPT_WORKERS)
    tlocal = threading.local()

    results: List[CompanyResult] = []
    lock = threading.Lock()

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS, thread_name_prefix="worker") as pool:
        futs = [
            pool.submit(process_one, nm, src, fetch_bucket, gpt_bucket, tlocal, progress_cb)
            for nm, src in zip(names, source_leis)
        ]
        total = len(futs)
        for i, fut in enumerate(as_completed(futs), start=1):
            try:
                res = fut.result()
                with lock:
                    results.append(res)
                if progress_cb:
                    progress_cb(f"[done] {i}/{total} {res.company_name}\n")
            except Exception as e:
                if progress_cb:
                    progress_cb(f"[error] worker failed: {e}\n")

    try:
        drv = getattr(tlocal, "driver", None)
        if drv:
            drv.quit()
    except Exception:
        pass

    out_df = pd.DataFrame([r.__dict__ for r in results])
    if not out_df.empty:
        out_df.rename(columns={
            "company_name": "Company Name",
            "source_lei": "Source LEI",
            "extracted_lei": "Extracted LEI",
            "match_status": "Match",
            "elapsed_sec": "Processing Time (s)",
            "search_url": "Search URL",
            "error": "Error",
        }, inplace=True)

    total = int(len(out_df))
    metrics = {
        "Total Companies": total,
        "Total Matches": int((out_df.get("Match", pd.Series()) == "Match").sum()) if total else 0,
        "Total Not Matches": int((out_df.get("Match", pd.Series()) == "Not Match").sum()) if total else 0,
        "Total Not Found LEIs": int((out_df.get("Extracted LEI", pd.Series()) == "Not found").sum()) if total else 0,
        "Total Errors": int((out_df.get("Extracted LEI", pd.Series()) == "Error").sum()) if total else 0,
        "Match %": float(round(((out_df.get("Match", pd.Series()) == "Match").mean() * 100.0), 2)) if total else 0.0,
        "Avg Proc Time (s)": float(round(out_df.get("Processing Time (s)", pd.Series(dtype=float)).mean() or 0.0, 3)) if total else 0.0,
    }

    out_df.to_csv(RESULTS_FILE, index=False)
    logging.info("Saved results to %s", RESULTS_FILE)
    return out_df, metrics

# -------------------------- Tkinter UI --------------------------
class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("780x560")
        self.resizable(True, True)

        nb = ttk.Notebook(self)
        self.single_tab = ttk.Frame(nb)
        self.batch_tab = ttk.Frame(nb)
        nb.add(self.single_tab, text="Single Lookup")
        nb.add(self.batch_tab, text="Batch Compare")
        nb.pack(fill=tk.BOTH, expand=True)

        self.build_single_tab()
        self.build_batch_tab()

    # ----- Single Lookup -----
    def build_single_tab(self) -> None:
        frame = self.single_tab
        pad = {"padx": 10, "pady": 10}

        ttk.Label(frame, text="Company Name").grid(row=0, column=0, sticky="w", **pad)
        self.entry_company = ttk.Entry(frame, width=50)
        self.entry_company.grid(row=0, column=1, sticky="we", **pad)

        self.status_single = tk.StringVar(value="Enter a company and click Lookup")
        ttk.Label(frame, textvariable=self.status_single, foreground="blue").grid(row=1, column=0, columnspan=3, sticky="w", **pad)

        self.output_single = tk.Text(frame, height=18, wrap="word")
        self.output_single.grid(row=2, column=0, columnspan=3, sticky="nsew", **pad)

        btn = ttk.Button(frame, text="Lookup", command=self.on_lookup)
        btn.grid(row=0, column=2, sticky="w", **pad)

        frame.grid_columnconfigure(1, weight=1)
        frame.grid_rowconfigure(2, weight=1)

    def on_lookup(self) -> None:
        company = self.entry_company.get().strip()
        if not company:
            self.status_single.set("Please enter a company name.")
            return

        self.status_single.set("Fetching...")
        self.output_single.delete("1.0", tk.END)

        def task():
            try:
                lei, elapsed = lookup_company_lei(company)
                self.after(0, lambda: self.show_single_result(company, lei, elapsed))
            except Exception as e:
                logging.exception("Single lookup failed: %s", e)
                self.after(0, lambda: self.status_single.set(f"Error: {e}"))

        threading.Thread(target=task, daemon=True).start()

    def show_single_result(self, company: str, lei: str, elapsed: float) -> None:
        self.output_single.insert(tk.END, f"LEI for {company}: {lei}\n")
        self.status_single.set(f"Done in {elapsed:.2f}s. Ready.")

    # ----- Batch Compare -----
    def build_batch_tab(self) -> None:
        frame = self.batch_tab
        pad = {"padx": 10, "pady": 10}

        ttk.Label(frame, text="Excel File (SourceData)").grid(row=0, column=0, sticky="w", **pad)
        self.path_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.path_var, width=60).grid(row=0, column=1, sticky="we", **pad)
        ttk.Button(frame, text="Browse", command=self.on_browse).grid(row=0, column=2, **pad)

        self.status_batch = tk.StringVar(value="Select a file and click Run Batch")
        ttk.Label(frame, textvariable=self.status_batch, foreground="blue").grid(row=1, column=0, columnspan=3, sticky="w", **pad)

        self.output_batch = tk.Text(frame, height=18, wrap="word")
        self.output_batch.grid(row=2, column=0, columnspan=3, sticky="nsew", **pad)

        self.btn_run = ttk.Button(frame, text="Run Batch", command=self.on_run_batch)
        self.btn_run.grid(row=0, column=3, **pad)

        self.progress = ttk.Progressbar(frame, mode="indeterminate")
        self.progress.grid(row=3, column=0, columnspan=4, sticky="we", **pad)

        frame.grid_columnconfigure(1, weight=1)
        frame.grid_rowconfigure(2, weight=1)

    def on_browse(self) -> None:
        path = filedialog.askopenfilename(title="Select SourceGLEIFData Excel File", filetypes=[("Excel Files", "*.xlsx")])
        if path:
            self.path_var.set(path)

    def on_run_batch(self) -> None:
        path = self.path_var.get().strip()
        if not path:
            messagebox.showwarning(APP_TITLE, "Please select an input Excel file.")
            return

        self.output_batch.delete("1.0", tk.END)
        self.status_batch.set("Running batch... streaming progress below.")
        self.progress.start(12)
        self.btn_run.config(state=tk.DISABLED)

        def task():
            try:
                def ui_append(msg: str) -> None:
                    self.after(0, lambda m=msg: (self.output_batch.insert(tk.END, m),
                                                 self.output_batch.see(tk.END)))
                out_df, metrics = run_batch(path, progress_cb=ui_append)

                def ui_update():
                    self.output_batch.insert(tk.END, "\n=== Final Results Table ===\n")
                    self.output_batch.insert(tk.END, out_df.to_string(index=False) + "\n\n")
                    self.output_batch.insert(tk.END, "Summary Metrics:\n")
                    for k, v in metrics.items():
                        self.output_batch.insert(tk.END, f" - {k}: {v}\n")
                    self.output_batch.insert(tk.END, f"\nSaved CSV: {RESULTS_FILE}\n")
                    self.status_batch.set("Batch complete.")
                self.after(0, ui_update)
            except Exception as e:
                logging.exception("Batch failed: %s", e)
                self.after(0, lambda: self.status_batch.set(f"Error: {e}"))
            finally:
                self.after(0, lambda: (self.progress.stop(), self.btn_run.config(state=tk.NORMAL)))

        threading.Thread(target=task, daemon=True).start()

# -------------------------- Entrypoint --------------------------
if __name__ == "__main__":
    app = App()
    app.mainloop()
