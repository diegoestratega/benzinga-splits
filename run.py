#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timezone

import yfinance as yf
from playwright.sync_api import sync_playwright

REPO_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(REPO_DIR, "data")
DATA_FILE = os.path.join(DATA_DIR, "splits.json")
DEBUG_SS  = os.path.join(REPO_DIR, "debug_screenshot.png")


# ── Scrape Benzinga ───────────────────────────────────────────────────────────

def scrape_benzinga():
    captured_xhr = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )

        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept-Language":           "en-US,en;q=0.9",
                "Sec-Fetch-Site":            "none",
                "Sec-Fetch-Mode":            "navigate",
                "Upgrade-Insecure-Requests": "1",
            },
        )

        page = ctx.new_page()

        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver',  { get: () => undefined });
            Object.defineProperty(navigator, 'plugins',    { get: () => [1, 2, 3] });
            Object.defineProperty(navigator, 'languages',  { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };
        """)

        def on_response(response):
            ct = response.headers.get("content-type", "")
            if "json" not in ct:
                return
            try:
                body = response.json()

                if isinstance(body, dict) and "splits" in body:
                    items = body.get("splits") or []
                    if len(items) > 0:
                        print(f"  ✓ XHR [splits] {len(items)} rows — {response.url[:70]}")
                        captured_xhr.extend(items)
                        return

                if isinstance(body, list) and len(body) > 2:
                    first = body[0]
                    if isinstance(first, dict):
                        keys = set(k.lower() for k in first.keys())
                        if keys & {"ticker", "symbol"} and keys & {"date", "date_ex", "ratio"}:
                            print(f"  ✓ XHR [list] {len(body)} rows — {response.url[:70]}")
                            captured_xhr.extend(body)
                            return

                if isinstance(body, dict):
                    for nk in ("data", "results", "calendar", "events", "items"):
                        sub = body.get(nk)
                        if isinstance(sub, list) and len(sub) > 2:
                            first = sub[0]
                            if isinstance(first, dict):
                                keys = set(k.lower() for k in first.keys())
                                if (keys & {"ticker", "symbol"}
                                        and keys & {"date", "date_ex", "ratio"}):
                                    print(f"  ✓ XHR [{nk}] {len(sub)} rows — {response.url[:70]}")
                                    captured_xhr.extend(sub)
                                    return
            except Exception:
                pass

        page.on("response", on_response)

        page.route(
            "**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,mp4,webp,avif}",
            lambda r: r.abort(),
        )

        print("→ Opening Benzinga splits calendar...")
        try:
            page.goto(
                "https://www.benzinga.com/calendars/stock-splits",
                wait_until="domcontentloaded",
                timeout=45000,
            )
        except Exception as e:
            print(f"  ⚠ Note: {e}")

        print("  Waiting for network to settle...")
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
            print("  ✓ Network idle")
        except Exception:
            print("  ⚠ Network still active — waiting 5s more...")
            page.wait_for_timeout(5000)

        if captured_xhr:
            browser.close()
            seen   = set()
            unique = []
            for r in captured_xhr:
                key = (str(r.get("ticker", ""))
                       + str(r.get("date_ex", ""))
                       + str(r.get("date", "")))
                if key not in seen:
                    seen.add(key)
                    unique.append(r)
            return unique, "xhr"

        print("  XHR not captured — DOM extraction with incremental scroll...")

        ROW_SEL = "[role='row']"

        try:
            page.wait_for_selector(ROW_SEL, timeout=15000)
        except Exception:
            print("  ✗ No rows found — saving debug screenshot")
            page.screenshot(path=DEBUG_SS, full_page=True)
            browser.close()
            return [], "none"

        def extract_visible_rows():
            return page.evaluate(
                """(sel) => {
                    const DATE_RE = /^\\d{1,2}\\/\\d{1,2}\\/\\d{4}$/;
                    const TICK_RE = /^[A-Z]{1,6}$/;
                    const allRows = Array.from(document.querySelectorAll(sel));
                    const results = [];
                    for (const row of allRows) {
                        const cells = Array.from(
                            row.querySelectorAll('[role="gridcell"], [role="cell"], td')
                        ).map(c => c.innerText.trim());
                        if (cells.length < 3) continue;
                        if (!DATE_RE.test(cells[0])) continue;
                        if (!TICK_RE.test(cells[2])) continue;
                        results.push({
                            date_ex : cells[0] || '',
                            name    : cells[1] || '',
                            ticker  : cells[2] || '',
                            ratio   : cells[4] || '',
                        });
                    }
                    return results;
                }""",
                ROW_SEL,
            )

        accumulated = {}

        def collect():
            rows = extract_visible_rows()
            new  = 0
            for r in rows:
                key = r["ticker"] + r["date_ex"]
                if key and key not in accumulated:
                    accumulated[key] = r
                    new += 1
            return new

        collect()
        print(f"  Initial collection: {len(accumulated)} rows")

        try:
            first_box = page.locator(ROW_SEL).first.bounding_box()
            if first_box:
                page.mouse.move(
                    first_box["x"] + first_box["width"]  / 2,
                    first_box["y"] + first_box["height"] / 2,
                )
        except Exception:
            pass

        prev_total   = 0
        stable_iters = 0

        print("  Scrolling and collecting rows...")
        for attempt in range(80):
            page.evaluate("""() => {
                const tagged = document.querySelector('[data-scroll-target="true"]');
                if (tagged) { tagged.scrollTop += 500; return; }
                const rows = document.querySelectorAll("[role='row']");
                if (!rows.length) return;
                let el = rows[rows.length - 1].parentElement;
                for (let d = 0; d < 12 && el && el !== document.body; d++) {
                    const oy = window.getComputedStyle(el).overflowY;
                    if ((oy === 'scroll' || oy === 'auto')
                            && el.scrollHeight > el.clientHeight) {
                        el.scrollTop += 500;
                        return;
                    }
                    el = el.parentElement;
                }
                rows[rows.length - 1].scrollIntoView(
                    { behavior: 'instant', block: 'end' }
                );
            }""")

            page.mouse.wheel(0, 500)
            page.wait_for_timeout(500)

            new_found = collect()
            total     = len(accumulated)
            print(f"    Scroll {attempt+1:>2}: +{new_found:>2} new  |  total: {total}")

            if total == prev_total:
                stable_iters += 1
                if stable_iters >= 4:
                    print("  ✓ No new rows for 4 iterations — scroll complete")
                    break
            else:
                stable_iters = 0
            prev_total = total

        rows_final = list(accumulated.values())
        print(f"  ✓ Total unique rows collected: {len(rows_final)}")
        browser.close()
        return rows_final, "dom"


# ── Date normalizer → YYYY-MM-DD ──────────────────────────────────────────────

def normalize_date(raw):
    if not raw:
        return ""
    raw = str(raw).strip()
    for fmt in (
        "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y",
        "%b %d, %Y", "%B %d, %Y", "%b. %d, %Y",
        "%m-%d-%Y", "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass
    return ""


# ── Row normalizer ────────────────────────────────────────────────────────────

def normalize_row(row, source):
    raw_opt = row.get("optionable")
    if isinstance(raw_opt, bool):
        opt = raw_opt
    elif isinstance(raw_opt, str):
        opt = raw_opt.strip().lower() in ("true", "yes", "1", "y", "✓")
    else:
        opt = None

    if source == "xhr":
        date_raw = (
            row.get("date_ex")
            or row.get("date")
            or row.get("date_distribution")
            or ""
        )
        ticker = str(row.get("ticker") or row.get("symbol") or "").strip().upper()
        name   = str(row.get("name",  "")).strip()
        ratio  = str(row.get("ratio", "")).strip()
    else:
        date_raw = str(row.get("date_ex") or "").strip()
        ticker   = str(row.get("ticker")  or "").strip().upper()
        name     = str(row.get("name",    "")).strip()
        ratio    = str(row.get("ratio",   "")).strip()

    return {
        "date_ex":    normalize_date(date_raw),
        "name":       name,
        "ticker":     ticker,
        "ratio":      ratio,
        "optionable": opt,
    }


# ── yfinance optionable check ─────────────────────────────────────────────────

def is_optionable(ticker):
    try:
        t           = yf.Ticker(ticker)
        expirations = t.options
        return len(expirations) > 0
    except Exception:
        return False


# ── Git commit + push ─────────────────────────────────────────────────────────

def git_push():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    subprocess.run(
        ["git", "branch", "-M", "main"],
        cwd=REPO_DIR, capture_output=True, text=True
    )

    cmds = [
        ["git", "add",    "."],
        ["git", "commit", "-m", f"update: splits [{ts}]"],
        ["git", "push",   "-u", "origin", "HEAD:main"],
    ]
    for cmd in cmds:
        out      = subprocess.run(cmd, cwd=REPO_DIR, capture_output=True, text=True)
        combined = (out.stdout + out.stderr).strip()
        if out.returncode != 0:
            if "nothing to commit" in combined or "nothing added" in combined:
                print("  ℹ No changes — data unchanged since last run")
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

    # 1 — Scrape
    raw, source = scrape_benzinga()
    print(f"\n  Source: [{source}]  Raw rows: {len(raw)}\n")

    if source == "none":
        print("✗ All extraction methods failed.")
        print("  Check debug_screenshot.png to see page state.")
        sys.exit(1)

    # 2 — Normalize + filter to today and forward
    seen   = set()
    future = []
    for row in raw:
        n      = normalize_row(row, source)
        ticker = re.sub(r"[^A-Z]", "", n["ticker"])
        if not ticker or len(ticker) > 6:
            continue
        if not n["date_ex"] or n["date_ex"] < today:
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        n["ticker"] = ticker
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

    # 3 — Optionable filter via yfinance
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

    # 4 — Sort + strip internal flag before saving
    final = sorted(optionable, key=lambda x: x["date_ex"])
    final = [{k: v for k, v in s.items() if k != "optionable"} for s in final]

    print(f"\n✓ {len(final)} optionable splits found")

    # 5 — Save JSON
    os.makedirs(DATA_DIR, exist_ok=True)
    payload = {
        "splits":     final,
        "today":      today,
        "updated_at": now_utc,
        "total":      len(final),
    }
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"✓ Saved → {DATA_FILE}\n")

    # 6 — Git push
    print("→ Pushing to GitHub...\n")
    ok = git_push()
    if ok:
        print("\n✓ Done — GitHub Pages updates in ~30 seconds.\n")
    else:
        print("\n⚠ JSON saved but git push failed.")
        print("  Run manually: git push origin main\n")


if __name__ == "__main__":
    main()
