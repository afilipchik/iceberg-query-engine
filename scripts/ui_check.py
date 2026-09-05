#!/usr/bin/env .venv/bin/python
"""Browser gate for the web UI (query-ui epic): drives every /ui view and the
SQL console against a running `serve` node with Playwright (Firefox), fails
on any browser console error or page error, and writes full-page screenshots
to .scratch/ui_check/.

    .venv/bin/pip install playwright && .venv/bin/playwright install firefox
    scripts/claude-safe-build.sh ./target/release/query_engine serve --data data/tpch-100mb &
    .venv/bin/python scripts/ui_check.py http://127.0.0.1:7777

Expects a node that has already run a few queries (the checks click the first
row and filter for `missing_table`); run `SELECT * FROM missing_table` once
before if the log is empty. The stray-"null" and sticky-header console bugs
(2026-09-05) were only visible through this gate, not static screenshots.
"""
import os
import sys
os.makedirs(".scratch/ui_check", exist_ok=True)
from playwright.sync_api import sync_playwright
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:7777"
errors = []
with sync_playwright() as p:
    b = p.firefox.launch()
    pg = b.new_page(viewport={"width": 1400, "height": 1000})
    pg.on("console", lambda m: errors.append(f"console.{m.type}: {m.text}") if m.type in ("error", "warning") else None)
    pg.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
    views = [("overview", "#/", ".tiles .tile"), ("queries", "#/queries", "table.data tbody tr"),
             ("stats", "#/stats", ".chart svg"), ("cluster", "#/cluster", "table.data tbody tr"),
             ("tables", "#/tables", "details.plan")]
    for name, h, sel in views:
        pg.goto(f"{BASE}/ui{h}"); pg.wait_for_selector(sel, timeout=10000)
        pg.screenshot(path=f".scratch/ui_check/{name}.png", full_page=True)
        print(f"{name}: ok ({pg.locator(sel).count()} x {sel!r})")
    # Queries -> click first row -> detail
    pg.goto(f"{BASE}/ui#/queries"); pg.wait_for_selector("table.data tbody tr")
    pg.locator("table.data tbody tr").first.click()
    pg.wait_for_selector(".facts .fact"); assert pg.url.split("#")[1].startswith("/query/")
    print("detail via row click: ok,", pg.locator(".facts .fact").count(), "facts,", "phases:", pg.locator(".phases .seg").count())
    # Filter box
    pg.goto(f"{BASE}/ui#/queries"); pg.wait_for_selector("table.data tbody tr")
    pg.fill(".filters input[type=text]", "missing_table"); pg.press(".filters input[type=text]", "Enter")
    pg.wait_for_function("document.querySelectorAll('table.data tbody tr').length === 1", timeout=5000)
    print("filter: ok ->", pg.locator("table.data tbody tr td.sql").first.inner_text()[:60])
    # SQL console: run a query, read status, follow the detail link
    pg.goto(f"{BASE}/ui#/sql"); pg.wait_for_selector(".console textarea")
    pg.fill(".console textarea", "SELECT n_name, n_regionkey FROM nation ORDER BY n_name LIMIT 7")
    pg.click("button.btn.primary"); pg.wait_for_selector(".results table.data tbody tr", timeout=15000)
    rows = pg.locator(".results table.data tbody tr").count(); status = pg.locator(".console .kv").inner_text()
    print(f"console: {rows} rows rendered; status: {status.replace(chr(10), ' | ')}")
    pg.screenshot(path=".scratch/ui_check/sql.png", full_page=True)
    pg.click(".console .kv a"); pg.wait_for_selector(".facts .fact")
    sql_shown = pg.locator(".sqlbox pre").inner_text()
    assert "FROM nation" in sql_shown, sql_shown
    print("console -> detail link: ok, front door", pg.locator(".facts .fact:has-text('front door') .v").inner_text())
    # failing query through the console
    pg.goto(f"{BASE}/ui#/sql"); pg.wait_for_selector(".console textarea")
    pg.fill(".console textarea", "SELECT nope FROM nation"); pg.keyboard.press("Control+Enter")
    pg.wait_for_selector(".console .errbox pre", timeout=15000)
    print("console error path: ok ->", pg.locator(".console .errbox pre").inner_text()[:70])
    # Auto-refresh pause + dark mode render
    pg.goto(f"{BASE}/ui#/"); pg.wait_for_selector(".tiles .tile"); pg.uncheck("#auto-refresh")
    assert pg.locator("#refresh-state").inner_text() == "paused"
    pg.emulate_media(color_scheme="dark"); pg.wait_for_timeout(300)
    pg.screenshot(path=".scratch/ui_check/overview-dark.png", full_page=True); print("dark mode + pause: ok")
    b.close()
print("browser errors:", errors if errors else "none")
sys.exit(1 if errors else 0)
