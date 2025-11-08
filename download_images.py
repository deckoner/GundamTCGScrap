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
    if not url or not isinstance(url, str):
        return ""
    base = os.path.basename(url)
    base = base.split(".webp")[0]
    return base.strip()


def get_image_extension(url: str) -> str:
    ext_match = re.search(r"\.(\w+)(?:\?|$)", url)
    return f".{ext_match.group(1)}" if ext_match else ".webp"


async def fetch_and_optimize_image(session, url: str, name: str, retry: int = 0):
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
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    urls = [u.strip() for u in df["img"].tolist() if u.strip()]
    unique_urls = list(dict.fromkeys(urls))  # eliminar duplicados

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

                if new_downloads > 0 and new_downloads % random.randint(5, 10) == 0:
                    sleep_time = random.randint(5, 18)
                    console.print(
                        f"[yellow]Pausa aleatoria de {sleep_time}s...[/yellow]"
                    )
                    await asyncio.sleep(sleep_time)

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
    import sys

    if len(sys.argv) < 2:
        console.print("[yellow]Uso:[/yellow] python download_images.py [archivo.csv]")
        sys.exit(1)

    csv_path = sys.argv[1]
    asyncio.run(download_all_images(csv_path))
