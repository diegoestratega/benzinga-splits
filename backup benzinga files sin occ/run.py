#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timezone

import yfinance as yf
from curl_cffi import requests as curl_requests

REPO_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(REPO_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "splits.json")
DEBUG_DIR = os.path.join(REPO_DIR, "debug")
BZ_URL    = "https://www.benzinga.com/calendars/stock-splits"

HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":  "document",
    "Sec-Fetch-Mode":  "navigate",
    "Sec-Fetch-Site":  "none",
    "Sec-Fetch-User":  "?1",
}


# ── Fetch page ────────────────────────────────────────────────────────────────

def fetch_page():
    print(f"→ Fetching Benzinga splits page (curl_cffi Chrome124)...")
    try:
        r = curl_requests.get(
            BZ_URL,
            headers=HEADERS,
            impersonate="chrome124",
            timeout=25,
        )
        print(f"  HTTP {r.status_code}  ({len(r.text):,} bytes)")

        if r.status_code != 200:
            print(f"  ✗ HTTP {r.status_code}")
            save_debug("error_page.html", r.text)
            return None

        if "Something went wrong" in r.text or "UH-OH" in r.text:
            print("  ✗ Got error page content")
            save_debug("error_200.html", r.text)
            return None

        print("  ✓ Page loaded successfully")
        save_debug("last_page.html", r.text)
        return r.text

    except Exception as e:
        print(f"  ✗ Request failed: {e}")
        return None


# ── Parse __NEXT_DATA__ (Next.js embedded JSON) ───────────────────────────────

def parse_next_data(html):
    print("  Trying __NEXT_DATA__ JSON extraction...")
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    if not m:
        print("  ✗ __NEXT_DATA__ not found")
        return None

    try:
        data = json.loads(m.group(1))
    except Exception as e:
        print(f"  ✗ JSON parse error: {e}")
        return None

    def find_splits(obj, depth=0):
        if depth > 15:
            return None
        if isinstance(obj, list) and len(obj) > 0:
            first = obj[0]
            if isinstance(first, dict):
                keys = set(k.lower() for k in first.keys())
                if keys & {"ticker", "symbol"} and keys & {"date_ex", "date", "ratio"}:
                    return obj
        if isinstance(obj, dict):
            for v in obj.values():
                result = find_splits(v, depth + 1)
                if result is not None:
                    return result
        return None

    splits = find_splits(data)
    if splits:
        print(f"  ✓ Found {len(splits)} splits in __NEXT_DATA__")
        return splits

    print("  ✗ No splits array in __NEXT_DATA__ — saving JSON for inspection")
    save_debug("next_data.json", json.dumps(data, indent=2))
    print(f"  Saved → {DEBUG_DIR}/next_data.json")
    return None


# ── Parse HTML table (fallback) ───────────────────────────────────────────────

def parse_html_table(html):
    print("  Trying HTML table extraction...")

    DATE_RE = re.compile(r"\d{2}/\d{2}/\d{4}")
    TICK_RE = re.compile(r"^[A-Z]{1,6}$")

    tables = re.findall(r"<table[\s\S]*?</table>", html, re.IGNORECASE)
    print(f"  Found {len(tables)} <table> elements")

    for t_html in tables:
        rows_raw = re.findall(r"<tr[\s\S]*?</tr>", t_html, re.IGNORECASE)
        rows = []
        for row in rows_raw:
            cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>",
                               row, re.IGNORECASE)
            cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
            cells = [c for c in cells if c]
            if cells:
                rows.append(cells)

        if len(rows) < 3:
            continue

        header  = rows[0]
        h_lower = [h.lower() for h in header]

        def col(keywords):
            for i, h in enumerate(h_lower):
                if any(k in h for k in keywords):
                    return i
            return -1

        i_date  = col(["ex-date", "ex date", "exdate"])
        i_sym   = col(["ticker", "symbol"])
        i_name  = col(["company", "name"])
        i_ratio = col(["ratio", "split"])
        i_opt   = col(["option"])

        if i_date == -1 or i_sym == -1:
            continue

        print(f"  ✓ Valid table found — headers: {header}")
        parsed = []
        for cells in rows[1:]:
            if len(cells) <= max(i_date, i_sym):
                continue
            d = cells[i_date]
            s = cells[i_sym].upper()
            if not DATE_RE.match(d) or not TICK_RE.match(s):
                continue
            parsed.append({
                "date_ex":    d,
                "ticker":     s,
                "name":       cells[i_name]  if 0 <= i_name  < len(cells) else "",
                "ratio":      cells[i_ratio] if 0 <= i_ratio < len(cells) else "",
                "optionable": cells[i_opt]   if 0 <= i_opt   < len(cells) else None,
            })

        if parsed:
            print(f"  ✓ {len(parsed)} rows extracted from HTML table")
            return parsed

    print("  ✗ No usable table found")
    return None


# ── Master scrape ─────────────────────────────────────────────────────────────

def scrape():
    html = fetch_page()
    if html is None:
        return [], "none"

    rows = parse_next_data(html)
    if rows is not None:
        return rows, "next_data"

    rows = parse_html_table(html)
    if rows is not None:
        return rows, "html_table"

    print(f"\n  ✗ Could not extract data — inspect {DEBUG_DIR}/last_page.html")
    return [], "none"


# ── Helpers ───────────────────────────────────────────────────────────────────

def save_debug(filename, content):
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        with open(os.path.join(DEBUG_DIR, filename), "w",
                  encoding="utf-8", errors="replace") as f:
            f.write(content)
    except Exception:
        pass


def normalize_date(raw):
    if not raw:
        return ""
    raw = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y",
                "%b %d, %Y", "%B %d, %Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass
    return ""


def normalize_row(row):
    raw_opt = row.get("optionable")
    if isinstance(raw_opt, bool):
        opt = raw_opt
    elif isinstance(raw_opt, str):
        opt = raw_opt.strip().lower() in ("true", "yes", "1", "y")
    else:
        opt = None

    return {
        "date_ex":    normalize_date(str(row.get("date_ex") or row.get("date") or "")),
        "name":       str(row.get("name",   "") or "").strip(),
        "ticker":     re.sub(r"[^A-Z]", "", str(row.get("ticker", "") or "").upper()),
        "ratio":      str(row.get("ratio",  "") or "").strip(),
        "optionable": opt,
    }


def is_optionable(ticker):
    try:
        return len(yf.Ticker(ticker).options) > 0
    except Exception:
        return False


def git_push():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subprocess.run(["git", "branch", "-M", "main"],
                   cwd=REPO_DIR, capture_output=True, text=True)
    for cmd in [
        ["git", "add",    "."],
        ["git", "commit", "-m", f"update: splits [{ts}]"],
        ["git", "push",   "-u", "origin", "HEAD:main"],
    ]:
        out      = subprocess.run(cmd, cwd=REPO_DIR, capture_output=True, text=True)
        combined = (out.stdout + out.stderr).strip()
        if out.returncode != 0:
            if "nothing to commit" in combined or "nothing added" in combined:
                print("  ℹ No changes — data unchanged")
                return True
            print(f"  ✗ git error: {combined}")
            return False
        print(f"  ✓ {' '.join(cmd[:2])}")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    today   = date.today().isoformat()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'═' * 56}")
    print(f"  Benzinga Splits Scraper — {now_utc}")
    print(f"  Filtering from: {today} forward")
    print(f"{'═' * 56}\n")

    raw, source = scrape()
    print(f"\n  Source: [{source}]  Raw rows: {len(raw)}\n")

    if source == "none":
        print("✗ All extraction methods failed.")
        print(f"  Check {DEBUG_DIR}/ for debug files.")
        sys.exit(1)

    seen, future = set(), []
    for row in raw:
        n = normalize_row(row)
        if not n["ticker"] or len(n["ticker"]) > 6:
            continue
        if not n["date_ex"] or n["date_ex"] < today:
            continue
        if n["ticker"] in seen:
            continue
        seen.add(n["ticker"])
        future.append(n)

    future.sort(key=lambda x: x["date_ex"])
    print(f"→ {len(future)} splits from {today} forward")

    if not future:
        print("  Nothing to process — saving empty result.")
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump({"splits": [], "today": today,
                       "updated_at": now_utc, "total": 0}, f, indent=2)
        git_push()
        return

    known_yes = [s for s in future if s["optionable"] is True]
    known_no  = [s for s in future if s["optionable"] is False]
    unknown   = [s for s in future if s["optionable"] is None]

    print(f"  ✓ source confirms optionable : {len(known_yes)}")
    print(f"  ✗ source confirms NOT        : {len(known_no)}")
    print(f"  ? needs yfinance check       : {len(unknown)}\n")

    optionable = list(known_yes)

    if unknown:
        print(f"→ Checking {len(unknown)} tickers via yfinance...\n")
        for i, s in enumerate(unknown):
            result = is_optionable(s["ticker"])
            print(f"  [{i+1:>3}/{len(unknown)}] {s['ticker']:<8} "
                  f"{'✓ optionable' if result else '✗ skip'}")
            if result:
                s["optionable"] = True
                optionable.append(s)
            time.sleep(0.25)

    final = sorted(optionable, key=lambda x: x["date_ex"])
    final = [{k: v for k, v in s.items() if k != "optionable"} for s in final]

    print(f"\n✓ {len(final)} optionable splits found")

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump({"splits": final, "today": today,
                   "updated_at": now_utc, "total": len(final)}, f, indent=2)
    print(f"✓ Saved → {DATA_FILE}\n")

    print("→ Pushing to GitHub...\n")
    ok = git_push()
    if ok:
        print("\n✓ Done — GitHub Pages updates in ~30 seconds.\n")
    else:
        print("\n⚠ JSON saved but git push failed.")
        print("  Run manually: git push origin main\n")


if __name__ == "__main__":
    main()