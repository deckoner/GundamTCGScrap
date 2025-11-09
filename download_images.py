import os
import re
import asyncio
import random
import aiohttp
import pandas as pd
from io import BytesIO
from PIL import Image
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeRemainingColumn


console = Console()

OUTPUT_DIR = "images"
CONCURRENT_DOWNLOADS = 8
IMAGE_QUALITY = 85
RETRY_LIMIT = 3


def clean_image_name(url: str) -> str:
    """
    Limpia y genera un nombre de archivo válido a partir de una URL de imagen.

    Args:
        url (str): URL completa de la imagen.

    Returns:
        str: Nombre base del archivo sin extensión ni parámetros.
    """
    if not url or not isinstance(url, str):
        return ""
    base = os.path.basename(url)
    base = base.split(".webp")[0]
    return base.strip()


def get_image_extension(url: str) -> str:
    """
    Obtiene la extensión del archivo a partir de la URL.

    Args:
        url (str): URL de la imagen.

    Returns:
        str: Extensión del archivo (por ejemplo, '.jpg', '.png', '.webp').
    """
    ext_match = re.search(r"\.(\w+)(?:\?|$)", url)
    return f".{ext_match.group(1)}" if ext_match else ".webp"


async def fetch_and_optimize_image(session, url: str, name: str, retry: int = 0):
    """
    Descarga una imagen desde la URL, la convierte a formato WEBP optimizado y la guarda en disco.

    Si ocurre un fallo en la descarga, se reintenta automáticamente varias veces con pausas crecientes.

    Args:
        session (aiohttp.ClientSession): Sesión HTTP activa para realizar la solicitud.
        url (str): URL de la imagen.
        name (str): Nombre base para el archivo resultante.
        retry (int): Número de reintentos realizados hasta el momento.

    Returns:
        bool | None:
            - True si la descarga fue exitosa.
            - False si falló tras varios intentos.
            - None si la imagen ya existía y no se descargó nuevamente.
    """
    ext = get_image_extension(url)
    output_path = os.path.join(OUTPUT_DIR, name + ext)

    if os.path.exists(output_path):
        return None

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as resp:
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status}")

            content = await resp.read()
            image = Image.open(BytesIO(content)).convert("RGB")
            image.save(output_path, format="WEBP", optimize=True, quality=IMAGE_QUALITY)
            return True

    except Exception as e:
        if retry < RETRY_LIMIT:
            await asyncio.sleep(3 + retry)
            return await fetch_and_optimize_image(session, url, name, retry + 1)
        else:
            console.print(f"[red]Fallo persistente al descargar {url} → {e}[/red]")
            return False


async def download_all_images(csv_path: str):
    """
    Descarga y optimiza todas las imágenes de cartas listadas en un archivo CSV.

    Este proceso lee el CSV generado por el scraper, descarga las imágenes únicas en paralelo,
    las convierte a formato WEBP y realiza pausas aleatorias para evitar bloqueos por exceso de peticiones.

    Args:
        csv_path (str): Ruta del archivo CSV que contiene la columna 'img' con las URLs.

    Efectos secundarios:
        - Crea la carpeta 'images' si no existe.
        - Guarda las imágenes procesadas en el directorio configurado.
        - Muestra una barra de progreso con el estado de descarga.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    urls = [u.strip() for u in df["img"].tolist() if u.strip()]
    unique_urls = list(dict.fromkeys(urls))  # Eliminar duplicados

    console.print(f"[cyan]Descargando {len(unique_urls)} imágenes...[/cyan]")

    connector = aiohttp.TCPConnector(limit=CONCURRENT_DOWNLOADS, ssl=False)
    timeout = aiohttp.ClientTimeout(total=40)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "{task.completed}/{task.total}",
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Descargando imágenes", total=len(unique_urls))
            failed_urls = []
            new_downloads = 0

            for url in unique_urls:
                name = clean_image_name(url)
                if not name:
                    continue

                result = await fetch_and_optimize_image(session, url, name)
                progress.advance(task_id)

                if result is True:
                    new_downloads += 1
                elif result is False:
                    failed_urls.append(url)

                # Pausas aleatorias
                if new_downloads > 0 and new_downloads % random.randint(5, 10) == 0:
                    sleep_time = random.randint(5, 18)
                    console.print(
                        f"[yellow]Pausa aleatoria de {sleep_time}s...[/yellow]"
                    )
                    await asyncio.sleep(sleep_time)

            # Segundo intento con las imágenes fallidas
            if failed_urls:
                console.print(
                    f"[magenta]Reintentando {len(failed_urls)} imágenes fallidas...[/magenta]"
                )
                for url in failed_urls:
                    name = clean_image_name(url)
                    await fetch_and_optimize_image(session, url, name)

    console.print(
        f"[green]Descarga completada. Imágenes guardadas en '{OUTPUT_DIR}/'[/green]"
    )


if __name__ == "__main__":
    """
    Punto de entrada del script.

    Uso:
        python download_images.py archivo.csv

    Si se ejecuta directamente, este bloque toma como argumento la ruta del archivo CSV
    con las URLs de imágenes y lanza la descarga asincrónica de todas ellas.
    """
    import sys

    if len(sys.argv) < 2:
        console.print("[yellow]Uso:[/yellow] python download_images.py [archivo.csv]")
        sys.exit(1)

    csv_path = sys.argv[1]
    asyncio.run(download_all_images(csv_path))
