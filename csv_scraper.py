import csv
import re
import time
import os
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
from rich.console import Console

console = Console()

START_URL = "https://www.gundam-gcg.com/en/cards/"
OUTPUT_CSV = "gundam_cards.csv"
HEADLESS = False
WAIT_BETWEEN_PACKAGES = 30
MAX_RETRIES_PER_PACKAGE = 3
PAUSE_EVERY_CARDS = 50
PAUSE_TIME = 30

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
    "trait",
    "link",
    "ap",
    "hp",
    "anime",
    "belongs_gd",
    "img",
]


def _find_dd_by_dt(frame, wanted):
    """
    Busca en un elemento <dl> el valor (<dd>) correspondiente a un título (<dt>) específico.

    Args:
        frame: Objeto de frame de Playwright en el que buscar.
        wanted (str): Texto del <dt> que se desea encontrar.

    Returns:
        str | None: El valor encontrado en el <dd>, o None si no se encuentra.
    """
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
    """
    Espera y obtiene el iframe que contiene el detalle de una carta.

    Args:
        page: Objeto Page de Playwright.
        timeout (int): Tiempo máximo de espera en milisegundos.

    Returns:
        Frame: El frame con el contenido de detalle.
    """
    iframe_el = page.wait_for_selector('iframe[src*="detail.php"]', timeout=timeout)
    return iframe_el.content_frame()


def _extract_from_frame(frame, base_url):
    """
    Extrae los datos relevantes de una carta desde el frame de detalle.

    Args:
        frame: Frame de Playwright con la información de la carta.
        base_url (str): URL base del sitio, usada para completar URLs relativas.

    Returns:
        dict: Diccionario con los datos de la carta.
    """

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
        re.sub(r"[.,\"\t\n\r']", "", txt_el.inner_text().strip()) if txt_el else None
    )
    level = _find_dd_by_dt(frame, "Lv.")
    cost = _find_dd_by_dt(frame, "COST")
    color = _find_dd_by_dt(frame, "COLOR")
    type_ = _find_dd_by_dt(frame, "TYPE")
    zone = _find_dd_by_dt(frame, "Zone")

    trait_text = _find_dd_by_dt(frame, "Trait")
    traits = None
    if trait_text:
        matches = re.findall(r"\((.*?)\)", trait_text)
        if matches:
            traits = " y ".join(matches)

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
        "trait": traits,
        "link": link,
        "ap": ap,
        "hp": hp,
        "anime": anime,
        "belongs_gd": belongs_gd,
        "img": img_url,
    }


def _reject_cookies(page):
    """
    Intenta rechazar las cookies del sitio si el botón correspondiente está disponible.

    Args:
        page: Página actual de Playwright.
    """
    try:
        reject_btn = page.locator("#onetrust-reject-all-handler")
        reject_btn.wait_for(timeout=8000)
        reject_btn.scroll_into_view_if_needed()
        reject_btn.click(force=True)
        page.wait_for_timeout(1000)
        console.print("[green]✅ Cookies rechazadas correctamente.[/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ No se pudieron rechazar las cookies: {e}[/yellow]")


def _open_dropdown(page):
    """
    Abre el menú desplegable de expansiones para mostrar las opciones disponibles.

    Args:
        page: Página de Playwright actual.
    """
    try:
        toggle = page.locator(
            '.toggleBtn.js-toggle[data-toggleelem="js-toggle--01"]'
        ).first
        toggle.scroll_into_view_if_needed()
        toggle.click(force=True)
        page.wait_for_timeout(1000)
        if not page.locator(".filterListItems").count():
            page.wait_for_selector(".filterListItems", timeout=5000)
        console.print("[green]✅ Menú de expansiones abierto correctamente.[/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️ No se pudo abrir el menú desplegable: {e}[/yellow]")


def _get_packages(page):
    """
    Obtiene la lista de paquetes o expansiones disponibles desde la página.

    Args:
        page: Página de Playwright.

    Returns:
        list[dict]: Lista de paquetes con su texto, valor y estado.
    """
    pkgs = page.evaluate(
        """() => {
            return Array.from(document.querySelectorAll('a.js-selectBtn-package')).map(a => ({
                text: a.textContent.trim(),
                val: a.getAttribute('data-val') || '',
                isCurrent: a.classList.contains('is-current')
            }));
        }"""
    )
    return pkgs


def _select_package(page, data_val, visible_text=None):
    """
    Selecciona un paquete por su valor interno o texto visible.

    Args:
        page: Página de Playwright.
        data_val (str): Valor del atributo 'data-val' del paquete.
        visible_text (str, opcional): Texto visible del paquete.
    """
    if data_val:
        page.evaluate(
            """(val) => {
                const a = Array.from(document.querySelectorAll('a.js-selectBtn-package')).find(x => (x.getAttribute('data-val') || '') === val);
                if (a) a.click();
            }""",
            data_val,
        )
    elif visible_text:
        page.evaluate(
            """(txt) => {
                const a = Array.from(document.querySelectorAll('a.js-selectBtn-package')).find(x => x.textContent.trim() === txt);
                if (a) a.click();
            }""",
            visible_text,
        )
    page.wait_for_timeout(800)


def _click_first_card(page):
    """
    Hace clic en la primera carta de la lista para abrir su detalle.

    Args:
        page: Página de Playwright.
    """
    page.wait_for_selector("li.cardItem a.cardStr[data-fancybox]", timeout=15000)
    first_card_anchor = page.locator("li.cardItem a.cardStr").first
    first_card_anchor.click()


def _close_fancybox_if_open(page):
    """
    Cierra el cuadro de diálogo (fancybox) si está abierto.

    Args:
        page: Página de Playwright.
    """
    try:
        close_btn = page.locator('button.fancybox-button[title="Close"]').first
        if close_btn and close_btn.is_visible():
            close_btn.click()
            page.wait_for_timeout(500)
    except Exception:
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(300)
        except Exception:
            pass


def _extract_first_card(page, base_url):
    """
    Extrae la información de la primera carta del paquete seleccionado.

    Args:
        page: Página de Playwright.
        base_url (str): URL base del sitio.

    Returns:
        tuple: (datos de la carta, clave única de la carta)
    """
    frame = _get_detail_frame(page, timeout=10000)
    first_data = _extract_from_frame(frame, base_url)
    key = (
        first_data["GD"],
        first_data["name"],
        first_data["rarity"],
        first_data["belongs_gd"],
    )
    console.print(f"[cyan]Primera carta encontrada: {key}[/cyan]")
    return first_data, key


def _iterate_cards(page, base_url, first_key, first_data):
    """
    Itera sobre todas las cartas de un paquete, navegando con el botón "Next".

    Args:
        page: Página actual.
        base_url (str): URL base para construir enlaces de imágenes.
        first_key (tuple): Clave de la primera carta.
        first_data (dict): Datos de la primera carta.

    Returns:
        list[dict]: Lista de todas las cartas del paquete.
    """
    records = [first_data]
    prev_key = first_key
    count_since_pause = 1

    while True:
        try:
            next_btn = page.locator('button.fancybox-button[title="Next"]').first
            if (
                not next_btn
                or not next_btn.is_visible()
                or not next_btn.is_enabled()
                or next_btn.get_attribute("disabled") is not None
            ):
                console.print("[green]✅ No hay más cartas en el paquete.[/green]")
                break

            next_btn.scroll_into_view_if_needed()
            next_btn.click()
        except Exception as e:
            console.print(f"[red]Error al hacer clic en 'Next': {e}[/red]")
            break

        start_time = time.time()
        loaded = False
        while time.time() - start_time < 15:
            try:
                frame = _get_detail_frame(page, timeout=5000)
                gd = frame.query_selector(".cardNo")
                name = frame.query_selector(".cardName")
                rarity = frame.query_selector(".rarity")
                belongs = frame.query_selector('dl dt:has-text("Where to get it") + dd')
                if gd and name and rarity:
                    cur_key = (
                        gd.inner_text().strip(),
                        name.inner_text().strip(),
                        rarity.inner_text().strip(),
                        belongs.inner_text().strip() if belongs else "",
                    )
                    if cur_key != prev_key:
                        loaded = True
                        break
            except Exception:
                pass
            time.sleep(0.1)

        if not loaded:
            console.print("[yellow]Tiempo de espera agotado para nueva carta.[/yellow]")
            break

        try:
            frame = _get_detail_frame(page, timeout=10000)
            cur = _extract_from_frame(frame, base_url)
        except Exception as e:
            console.print(f"[yellow]Error extrayendo carta: {e}[/yellow]")
            continue

        records.append(cur)
        prev_key = (
            cur["GD"],
            cur["name"],
            cur["rarity"],
            cur["belongs_gd"],
        )
        console.print(f"[green]Carta #{len(records)}: {prev_key}[/green]")

        count_since_pause += 1
        if count_since_pause >= PAUSE_EVERY_CARDS:
            console.print(
                f"[cyan]Pausa de {PAUSE_TIME}s para evitar sobrecarga.[/cyan]"
            )
            time.sleep(PAUSE_TIME)
            count_since_pause = 0

    _close_fancybox_if_open(page)
    return records


def _load_existing_csv():
    """
    Carga el archivo CSV existente con los datos previamente extraídos.

    Returns:
        list[dict]: Registros cargados desde el archivo.
    """
    if not os.path.exists(OUTPUT_CSV):
        return []
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _save_to_csv(records):
    """
    Guarda los registros de cartas en un archivo CSV.

    Args:
        records (list[dict]): Lista de cartas a guardar.
    """
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in records:
            row = {
                field: (r.get(field) if r.get(field) not in (None, "") else "NULL")
                for field in FIELDNAMES
            }
            writer.writerow(row)
    console.print(
        f"[green]✅ {len(records)} registros guardados en {OUTPUT_CSV}[/green]"
    )


def run_scraper(output_csv):
    """
    Ejecuta el proceso completo de scraping de cartas desde la página de Gundam GCG.

    Este proceso recorre cada expansión disponible, abre cada carta, extrae sus datos
    y los guarda en un archivo CSV. Además, puede reanudar desde donde se dejó.

    Args:
        output_csv (str): Ruta del archivo CSV donde se guardarán los resultados.
    """
    global OUTPUT_CSV
    OUTPUT_CSV = output_csv
    existing_records = _load_existing_csv()
    existing_packages = set(
        r["belongs_gd"] for r in existing_records if r.get("belongs_gd")
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--start-maximized"])
        page = browser.new_page(viewport={"width": 1920, "height": 1080})
        page.goto(START_URL, wait_until="domcontentloaded")
        _reject_cookies(page)

        _open_dropdown(page)
        packages = _get_packages(page)
        if not packages:
            console.print("[red]No se encontraron paquetes, finalizando.[/red]")
            browser.close()
            return

        all_records = existing_records
        base_url = page.url.rsplit("/", 1)[0] + "/"

        for pkg in packages:
            pkg_text = pkg["text"]
            pkg_val = pkg["val"]
            if pkg_text.strip().upper() == "ALL" or pkg_text in existing_packages:
                console.print(f"[yellow]Paquete '{pkg_text}' omitido.[/yellow]")
                continue

            console.print(f"[magenta]Procesando paquete: {pkg_text}[/magenta]")
            succeeded = False
            attempts = 0

            while not succeeded and attempts < MAX_RETRIES_PER_PACKAGE:
                attempts += 1
                temp_records = []
                try:
                    _open_dropdown(page)
                    _select_package(page, pkg_val, pkg_text)
                    try:
                        page.wait_for_selector(
                            "li.cardItem a.cardStr[data-fancybox]", timeout=15000
                        )
                    except Exception:
                        console.print(
                            f"[yellow]No se encontraron cartas en '{pkg_text}'.[/yellow]"
                        )
                        succeeded = True
                        break

                    _click_first_card(page)
                    first_data, first_key = _extract_first_card(page, base_url)
                    temp_records = _iterate_cards(page, base_url, first_key, first_data)

                    for r in temp_records:
                        if not r.get("belongs_gd"):
                            r["belongs_gd"] = pkg_text

                    all_records = [
                        r for r in all_records if r.get("belongs_gd") != pkg_text
                    ]
                    all_records.extend(temp_records)
                    _save_to_csv(all_records)

                    console.print(
                        f"[green]Paquete '{pkg_text}' procesado correctamente ({len(temp_records)} cartas).[/green]"
                    )
                    succeeded = True

                    console.print(
                        f"[cyan]Esperando {WAIT_BETWEEN_PACKAGES}s antes del siguiente paquete...[/cyan]"
                    )
                    time.sleep(WAIT_BETWEEN_PACKAGES)
                except Exception as e:
                    console.print(
                        f"[red]Error en paquete '{pkg_text}' (intento {attempts}): {e}[/red]"
                    )
                    _close_fancybox_if_open(page)
                    all_records = [
                        r for r in all_records if r.get("belongs_gd") != pkg_text
                    ]
                    _save_to_csv(all_records)
                    console.print(
                        f"[yellow]Reintentando '{pkg_text}' después de {WAIT_BETWEEN_PACKAGES}s...[/yellow]"
                    )
                    time.sleep(WAIT_BETWEEN_PACKAGES)

            if not succeeded:
                console.print(
                    f"[red]No se pudo completar '{pkg_text}' tras varios intentos.[/red]"
                )

        _save_to_csv(all_records)
        browser.close()
