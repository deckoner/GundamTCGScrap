import csv
import re
import time
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from rich.console import Console
from rich.progress import Progress

console = Console()

START_URL = "https://www.gundam-gcg.com/en/cards/"
OUTPUT_CSV = "gundam_cards.csv"
HEADLESS = True

FIELDNAMES = [
    "GD",
    "name",
    "rarity",
    "level",
    "cost",
    "color",
    "type",
    "text_card",
    "zone",
    "link",
    "ap",
    "hp",
    "anime",
    "belongs_gd",
    "img",
]


def _find_dd_by_dt(frame, wanted):
    dls = frame.query_selector_all("dl")
    for dl in dls:
        dt = dl.query_selector("dt")
        dd = dl.query_selector("dd")
        if not dt or not dd:
            continue
        try:
            dt_text = dt.inner_text().strip()
        except Exception:
            continue
        if dt_text == wanted:
            try:
                value = dd.inner_text().strip()
            except Exception:
                return None
            return None if value == "-" else value
    return None


def _get_detail_frame(page, timeout=10000):
    iframe_el = page.wait_for_selector('iframe[src*="detail.php"]', timeout=timeout)
    return iframe_el.content_frame()


def _extract_from_frame(frame, base_url):
    def safe_text(selector):
        el = frame.query_selector(selector)
        if not el:
            return None
        text = el.inner_text().strip()
        return None if text == "-" else text

    gd = safe_text(".cardNo") or safe_text("div.cardNo")
    rarity = safe_text(".rarity")
    name = safe_text(".cardName") or safe_text(".nameCol h1.cardName")

    txt_el = frame.query_selector("div.cardDataRow.overview .dataTxt")
    text_card = (
        re.sub(r"[.,\"\t\n\r\']", "", txt_el.inner_text().strip()) if txt_el else None
    )

    level = _find_dd_by_dt(frame, "Lv.")
    cost = _find_dd_by_dt(frame, "COST")
    color = _find_dd_by_dt(frame, "COLOR")
    type_ = _find_dd_by_dt(frame, "TYPE")
    zone = _find_dd_by_dt(frame, "Zone")
    link = _find_dd_by_dt(frame, "Link")
    ap = _find_dd_by_dt(frame, "AP")
    hp = _find_dd_by_dt(frame, "HP")
    anime = _find_dd_by_dt(frame, "Source Title")
    belongs_gd = _find_dd_by_dt(frame, "Where to get it")

    img_el = frame.query_selector(
        "img[src*='/images/cards/card/'], img.cardImg, .cardImg img"
    )
    img_url = ""
    if img_el:
        src = img_el.get_attribute("src") or img_el.get_attribute("data-src") or ""
        if src:
            img_url = urljoin(base_url, src)

    return {
        "GD": gd,
        "name": name,
        "rarity": rarity,
        "level": level,
        "cost": cost,
        "color": color,
        "type": type_,
        "text_card": text_card,
        "zone": zone,
        "link": link,
        "ap": ap,
        "hp": hp,
        "anime": anime,
        "belongs_gd": belongs_gd,
        "img": img_url,
    }


def _reject_cookies(page):
    try:
        reject_btn = page.locator("#onetrust-reject-all-handler")
        reject_btn.wait_for(timeout=8000)
        reject_btn.scroll_into_view_if_needed()
        reject_btn.click(force=True)
        page.wait_for_timeout(1000)
        console.print("[green]✅ Cookies rejected successfully.[/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Could not reject cookies: {e}[/yellow]")


def _open_dropdown(page):
    try:
        toggle = page.locator(
            '.toggleBtn.js-toggle[data-toggleelem="js-toggle--01"]'
        ).first
        toggle.scroll_into_view_if_needed()
        toggle.click(force=True)
        page.wait_for_timeout(3000)
        html_after_click = page.content()
        if "filterListItems" not in html_after_click:
            page.evaluate(
                "document.querySelector(\".toggleBtn.js-toggle[data-toggleelem='js-toggle--01']\").click()"
            )
            page.wait_for_timeout(3000)
        page.wait_for_selector(".filterListItems", timeout=5000)
        console.print("[green]✅ Expansion dropdown opened successfully.[/green]")
    except Exception as e:
        console.print(f"[red]❌ Could not open expansion dropdown: {e}[/red]")


def _select_all_option(page):
    try:
        page.wait_for_selector(".filterListItems", timeout=5000)
        page.evaluate(
            """
            const links = document.querySelectorAll('a.js-selectBtn-package');
            for (const link of links) {
                if (link.textContent.trim().toUpperCase() === 'ALL') {
                    link.scrollIntoView({behavior: 'instant', block: 'center'});
                    link.click();
                    break;
                }
            }
        """
        )
        page.wait_for_selector("li.cardItem a.cardStr[data-fancybox]", timeout=15000)
        console.print("[green]✅ 'ALL' selected successfully.[/green]")
    except Exception as e:
        console.print(f"[red]❌ Error selecting 'ALL': {e}[/red]")


def _click_first_card(page):
    first_card_anchor = page.locator("li.cardItem a.cardStr").first
    first_card_anchor.click()


def _extract_first_card(page, base_url):
    frame = _get_detail_frame(page, timeout=10000)
    first_data = _extract_from_frame(frame, base_url)
    key = (
        first_data["GD"],
        first_data["name"],
        first_data["rarity"],
        first_data["belongs_gd"],
    )
    console.print(f"[cyan]First card found: {key}[/cyan]")
    return first_data, key


def _wait_for_new_card(page, prev_key, timeout=15):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            frame = _get_detail_frame(page, timeout=5000)
            gd = frame.query_selector(".cardNo")
            name = frame.query_selector(".cardName")
            rarity = frame.query_selector(".rarity")
            belongs = frame.query_selector('dl dt:has-text("Where to get it") + dd')
            if not gd or not name or not rarity:
                continue
            key = (
                gd.inner_text().strip(),
                name.inner_text().strip(),
                rarity.inner_text().strip(),
                belongs.inner_text().strip() if belongs else "",
            )
            if key != prev_key:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def _iterate_cards(page, base_url, first_key, first_data):
    records = [first_data]
    prev_key = first_key

    while True:
        try:
            next_btn = page.locator('button.fancybox-button[title="Next"]').first
            next_btn.scroll_into_view_if_needed()
            next_btn.click()
        except Exception as e:
            console.print(f"[red]Could not click Next button: {e}[/red]")
            break

        if not _wait_for_new_card(page, prev_key, timeout=15):
            console.print("[yellow]Timeout waiting for a new card.[/yellow]")
            break

        try:
            frame = _get_detail_frame(page, timeout=10000)
            cur = _extract_from_frame(frame, base_url)
        except Exception as e:
            console.print(f"[yellow]Error extracting data: {e}[/yellow]")
            continue

        cur_key = (cur["GD"], cur["name"], cur["rarity"], cur["belongs_gd"])
        if cur_key == first_key:
            console.print("[green]Reached first card again. Scraping finished.[/green]")
            break

        records.append(cur)
        prev_key = cur_key
        console.print(f"[green]Scraping card #{len(records)}: {cur_key}[/green]")

    return records


def _save_to_csv(records):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in records:
            row = {}
            for field in FIELDNAMES:
                value = r.get(field)
                row[field] = value if value not in (None, "") else "NULL"
            writer.writerow(row)
    console.print(f"[green]✅ {len(records)} records saved to {OUTPUT_CSV}[/green]")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()
        page.goto(START_URL, wait_until="domcontentloaded")

        _reject_cookies(page)
        _open_dropdown(page)
        _select_all_option(page)
        _click_first_card(page)

        base_url = page.url.rsplit("/", 1)[0] + "/"
        first_data, first_key = _extract_first_card(page, base_url)
        records = _iterate_cards(page, base_url, first_key, first_data)
        _save_to_csv(records)

        browser.close()


if __name__ == "__main__":
    main()
