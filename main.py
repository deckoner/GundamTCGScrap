import csv
import time
import re
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Configuraciones
START_URL = "https://www.gundam-gcg.com/en/cards/"
OUTPUT_CSV = "gundam_cards.csv"
HEADLESS = True
WAIT_BETWEEN_CARDS = 0.6  # Tiempo de espera entre cartas (en segundos)

FIELDNAMES = [
    "GD", "name", "rarity", "level", "cost", "color", "type",
    "text_card", "zone", "link", "ap", "hp", "anime", "belongs_gd", "img"
]

def find_dd_by_dt(frame, wanted):
    dls = frame.query_selector_all("dl")
    for dl in dls:
        dt = dl.query_selector("dt")
        dd = dl.query_selector("dd")
        if not dt or not dd:
            continue
        dt_text = dt.inner_text().strip()
        if dt_text == wanted:
            value = dd.inner_text().strip()
            return None if value == "-" else value
    return None

def get_detail_frame(page, timeout=10000):
    iframe_el = page.wait_for_selector('iframe[src*="detail.php"]', timeout=timeout)
    return iframe_el.content_frame()

def extract_from_frame(frame, base_url):
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
    if txt_el:
        text_card = txt_el.inner_text().strip()
        # Limpiar caracteres problemáticos para CSV
        text_card = re.sub(r'[.,\"\t\n\r\']', '', text_card)
        text_card = text_card.strip()
    else:
        text_card = None

    level = find_dd_by_dt(frame, "Lv.")
    cost = find_dd_by_dt(frame, "COST")
    color = find_dd_by_dt(frame, "COLOR")
    type_ = find_dd_by_dt(frame, "TYPE")
    zone = find_dd_by_dt(frame, "Zone")
    link = find_dd_by_dt(frame, "Link")
    ap = find_dd_by_dt(frame, "AP")
    hp = find_dd_by_dt(frame, "HP")
    anime = find_dd_by_dt(frame, "Source Title")
    belongs_gd = find_dd_by_dt(frame, "Where to get it")

    img_el = frame.query_selector("img[src*='/images/cards/card/'], img.cardImg, .cardImg img")
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
        "img": img_url
    }

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()
        print(f"Abriendo {START_URL} ...")
        page.goto(START_URL, wait_until="domcontentloaded")

        # Abrir dropdown del paquete
        try:
            print("Abriendo menú de selección de expansiones...")
            toggle = page.locator('.toggleBtn.js-toggle[data-toggleelem="js-toggle--01"]').first
            toggle.click()
            page.wait_for_selector(".filterListItems", timeout=5000)
        except Exception as e:
            print("No se pudo abrir el menú de expansiones:", e)

        # Hacer clic en "ALL"
        try:
            all_btn = page.locator("a.js-selectBtn-package", has_text="ALL").first
            all_btn.scroll_into_view_if_needed()
            all_btn.click(force=True)

            # Esperar que la lista de cartas esté cargada
            page.wait_for_selector("li.cardItem a.cardStr[data-fancybox]", timeout=15000)
            print("Click en ALL hecho y lista de cartas actualizada.")
        except PWTimeout:
            print("Timeout esperando lista de cartas tras seleccionar ALL. Intentando continuar.")
        except Exception as e:
            print("Error al hacer clic en ALL:", e)

        # Click en la primera carta
        try:
            first_card_anchor = page.locator("li.cardItem a.cardStr").first
            first_card_anchor.click()
        except Exception as e:
            print("Error haciendo click en la primera carta:", e)
            browser.close()
            return

        try:
            detail_frame = get_detail_frame(page, timeout=10000)
        except PWTimeout:
            print("No se abrió el iframe de detalle. Abortando.")
            browser.close()
            return

        base_url = page.url.rsplit("/", 1)[0] + "/"

        print("Extrayendo primera carta...")
        first_data = extract_from_frame(detail_frame, base_url)
        first_key = (first_data["GD"], first_data["name"], first_data["rarity"], first_data["belongs_gd"])
        print("Primera carta encontrada:", first_key)

        records = [first_data]

        # Bucle de scraping
        while True:
            try:
                next_btn = page.locator('button.fancybox-button[title="Next"]').first
                next_btn.click()
            except Exception as e:
                print("No pude pulsar Next (fin o error):", e)
                break

            print(f"Esperando {WAIT_BETWEEN_CARDS} segundos para que cargue la siguiente carta...")
            time.sleep(WAIT_BETWEEN_CARDS)

            try:
                detail_frame = get_detail_frame(page, timeout=10000)
            except PWTimeout:
                print("Timeout esperando nuevo iframe tras Next. Rompiendo bucle.")
                break

            cur = extract_from_frame(detail_frame, base_url)
            cur_key = (cur["GD"], cur["name"], cur["rarity"], cur["belongs_gd"])

            if cur_key == first_key:
                print("Volvimos a la primera carta. Fin del scraping.")
                break

            print(f"Carta #{len(records)+1}: {cur_key}")
            records.append(cur)

        # Guardar CSV
        print(f"Guardando {len(records)} registros en {OUTPUT_CSV} ...")
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            for r in records:
                writer.writerow({k: r.get(k, "") if r.get(k, "") is not None else "" for k in FIELDNAMES})

        print("Hecho. CSV creado:", OUTPUT_CSV)
        browser.close()

if __name__ == "__main__":
    main()
