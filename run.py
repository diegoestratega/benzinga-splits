#!/usr/bin/env python3
import html
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone

import yfinance as yf
from curl_cffi import requests as curl_requests

REPO_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(REPO_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "splits.json")
DEBUG_DIR = os.path.join(REPO_DIR, "debug")
BZ_URL    = "https://www.benzinga.com/calendars/stock-splits"

OCC_URL   = "https://infomemo.theocc.com/infomemo/search-memo"
OCC_DAYS_BACK    = 60   # posted date window: today - N days -> today
OCC_DAYS_FORWARD = 60   # effective date window: today -> today + N days
OCC_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36")

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


# ── Fetch page (Benzinga) ───────────────────────────────────────────────────────

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


# ── Parse __NEXT_DATA__ (Next.js embedded JSON) ─────────────────────────────────

def parse_next_data(html_text):
    print("  Trying __NEXT_DATA__ JSON extraction...")
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html_text, re.DOTALL
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


# ── Parse HTML table (fallback, Benzinga) ───────────────────────────────────────

def parse_html_table(html_text):
    print("  Trying HTML table extraction...")

    DATE_RE = re.compile(r"\d{2}/\d{2}/\d{4}")
    TICK_RE = re.compile(r"^[A-Z]{1,6}$")

    tables = re.findall(r"<table[\s\S]*?</table>", html_text, re.IGNORECASE)
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


# ── Master scrape (Benzinga) ────────────────────────────────────────────────────

def scrape():
    html_text = fetch_page()
    if html_text is None:
        return [], "none"

    rows = parse_next_data(html_text)
    if rows is not None:
        return rows, "next_data"

    rows = parse_html_table(html_text)
    if rows is not None:
        return rows, "html_table"

    print(f"\n  ✗ Could not extract data — inspect {DEBUG_DIR}/last_page.html")
    return [], "none"


# ── OCC: fetch + parse Information Memos (Playwright — real browser) ───────────
#
# OCC's site sits behind Cloudflare bot protection which requires a genuine
# JS-executing browser to pass its challenge and obtain a valid cf_clearance
# cookie. A plain HTTP client (curl_cffi) cannot pass this, so we drive a
# real headless Chromium browser instead: load the page, fill the same
# filters used in the manual test, click Search, then parse the returned
# HTML exactly like before. Any failure here is fully non-fatal — Benzinga
# scraping and the git push always continue regardless of OCC's outcome.

def _occ_set_date(page, field_name, value):
    try:
        page.evaluate(
            """(args) => {
                const el = document.querySelector('input[name="' + args.name + '"]');
                if (el) {
                    el.value = args.value;
                    el.dispatchEvent(new Event('input',  { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }
            }""",
            {"name": field_name, "value": value}
        )
    except Exception:
        pass


def _occ_set_category(page, label_text, checked):
    try:
        cb = page.get_by_label(label_text, exact=False)
        if checked:
            cb.check(timeout=5000)
        else:
            cb.uncheck(timeout=5000)
    except Exception:
        pass


def fetch_occ_html():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  ✗ Playwright not installed — run: pip install playwright")
        print("    then: playwright install chromium")
        return None

    today      = date.today()
    start_post = (today - timedelta(days=OCC_DAYS_BACK)).strftime("%m/%d/%Y")
    end_post   = today.strftime("%m/%d/%Y")
    start_eff  = today.strftime("%m/%d/%Y")
    end_eff    = (today + timedelta(days=OCC_DAYS_FORWARD)).strftime("%m/%d/%Y")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=OCC_UA)
            page.goto(OCC_URL, timeout=45000, wait_until="domcontentloaded")

            # Allow Cloudflare's JS challenge to fully resolve.
            page.wait_for_timeout(4000)

            _occ_set_date(page, "startpostdate", start_post)
            _occ_set_date(page, "endpostdate",   end_post)
            _occ_set_date(page, "starteffdate",  start_eff)
            _occ_set_date(page, "endeffdate",    end_eff)

            _occ_set_category(page, "Contract Adjustment", True)
            _occ_set_category(page, "Options",             True)
            _occ_set_category(page, "Futures",             False)

            # Snapshot current results text so we can detect when it changes
            # after clicking Search — more reliable than waiting for total
            # network silence (networkidle), which never fires on pages with
            # background polling / analytics scripts.
            try:
                before_text = page.inner_text("body")
            except Exception:
                before_text = ""

            try:
                page.get_by_role("button", name=re.compile("search", re.I)).first.click(timeout=10000)
            except Exception as e:
                print(f"  ✗ OCC: could not click Search button: {e}")
                save_debug("occ_last_results.html", page.content())
                browser.close()
                return None

            # Poll for up to 20s until the results text actually changes,
            # instead of relying on a fixed wait or networkidle.
            changed = False
            for _ in range(20):
                page.wait_for_timeout(1000)
                try:
                    now_text = page.inner_text("body")
                except Exception:
                    now_text = before_text
                if now_text != before_text:
                    changed = True
                    break

            if not changed:
                print("  ⚠ OCC: results did not visibly change after search "
                      "(continuing with current page content)")

            html_text = page.content()
            browser.close()

        save_debug("occ_last_results.html", html_text)
        return html_text

    except Exception as e:
        print(f"  ✗ OCC Playwright session failed: {e}")
        return None


def parse_occ_html(html_text):
    """
    Strip tags to flat text, then walk repeating
    <memo#> <postdate> <effdate> <title...category> blocks.
    Extract only the effective date + first/current option symbol.
    """
    text = re.sub(r"<[^>]+>", " ", html_text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()

    header_re  = re.compile(r"(\d{4,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+")
    category_re = re.compile(
        r"\s*(?:Contract Adjustment|Options|Futures)(?:\s*\|\s*(?:Contract Adjustment|Options|Futures))*\s*$"
    )
    symbol_re = re.compile(r"Option Symbols?:\s*([A-Z][A-Z0-9]{0,9})")

    matches = list(header_re.finditer(text))
    if not matches:
        print("  ✗ OCC: no memo rows detected in response")
        return []

    results = []
    for i, m in enumerate(matches):
        eff_date = m.group(3)
        start = m.end()
        end   = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[start:end]
        title = category_re.sub("", block).strip()

        sym_m = symbol_re.search(title)
        if not sym_m:
            continue  # e.g. "Multiple Flex Position Consolidations" — no symbol

        ticker = sym_m.group(1)
        results.append({
            "date_ex": eff_date,
            "ticker":  ticker,
        })

    print(f"  ✓ OCC: {len(results)} memo rows with an option symbol parsed")
    return results


def scrape_occ():
    """Non-fatal: any failure here must never break the Benzinga pipeline."""
    print("→ Fetching OCC Information Memos (headless browser)...")
    try:
        html_text = fetch_occ_html()
        if not html_text:
            return []
        return parse_occ_html(html_text)
    except Exception as e:
        print(f"  ✗ OCC scrape failed (non-fatal): {e}")
        return []


# ── Helpers ──────────────────────────────────────────────────────────────────────

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


# ── Main ─────────────────────────────────────────────────────────────────────────

def main():
    today   = date.today().isoformat()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'═' * 56}")
    print(f"  Splits Scraper (Benzinga + OCC) — {now_utc}")
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

    bz_final = sorted(optionable, key=lambda x: x["date_ex"])
    bz_final = [{k: v for k, v in s.items() if k != "optionable"} for s in bz_final]
    for s in bz_final:
        s["source"] = "benzinga"

    print(f"\n✓ {len(bz_final)} optionable splits found (Benzinga)")

    # ── OCC: fill gaps only — never overrides or duplicates a Benzinga ticker ──
    bz_tickers = {s["ticker"] for s in bz_final}
    occ_raw    = scrape_occ()

    occ_seen, occ_final = set(), []
    for row in occ_raw:
        d = normalize_date(row.get("date_ex", ""))
        t = re.sub(r"[^A-Z0-9]", "", str(row.get("ticker", "")).upper())
        if not t or not d or d < today:
            continue
        if t in bz_tickers or t in occ_seen:
            continue
        occ_seen.add(t)
        occ_final.append({
            "date_ex": d,
            "name":    "",
            "ticker":  t,
            "ratio":   "",
            "source":  "occ",
        })

    occ_final.sort(key=lambda x: x["date_ex"])
    print(f"✓ {len(occ_final)} additional entries found (OCC, gap-fill only)")

    combined = sorted(bz_final + occ_final, key=lambda x: x["date_ex"])

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump({"splits": combined, "today": today,
                   "updated_at": now_utc, "total": len(combined)}, f, indent=2)
    print(f"\n✓ Saved → {DATA_FILE}  ({len(combined)} total entries)\n")

    print("→ Pushing to GitHub...\n")
    ok = git_push()
    if ok:
        print("\n✓ Done — GitHub Pages updates in ~30 seconds.\n")
    else:
        print("\n⚠ JSON saved but git push failed.")
        print("  Run manually: git push origin main\n")


if __name__ == "__main__":
    main()