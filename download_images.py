import csv
import os
import asyncio
import random
import boto3
import aiohttp

from io import BytesIO
from PIL import Image
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeRemainingColumn

console = Console()

CONCURRENT_DOWNLOADS = 8
IMAGE_QUALITY = 85
RETRY_LIMIT = 3
WEBP_KEY_EXT = ".webp"
CACHE_CONTROL = "public, max-age=31536000, immutable"

# Paquete que debe procesarse siempre primero: contiene las cartas básicas
# de recurso (R-001, EXB-001, EXR-001), necesarias para el resto de sets.
PRIORITY_CSV_KEYWORD = "basic cards"


def csv_sort_key(filename):
    """
    Clave de ordenación para que el CSV de 'Basic Cards' se procese primero.

    Args:
        filename (str): Nombre del archivo CSV.

    Returns:
        tuple: (0, nombre) para Basic Cards y (1, nombre) para el resto.
    """
    is_priority = PRIORITY_CSV_KEYWORD in filename.lower()
    return (0, filename) if is_priority else (1, filename)


def clean_image_name(url: str) -> str:
    """
    Limpia y genera el nombre base de un archivo a partir de una URL de imagen.

    Args:
        url (str): URL completa de la imagen.

    Returns:
        str: Nombre base sin extensión ni parámetros.
    """
    if not url or not isinstance(url, str):
        return ""
    base = os.path.basename(url)
    base = base.split(".webp")[0]
    return base.strip()


def load_r2_client():
    """
    Carga la configuración de Cloudflare R2 desde el archivo .env y crea el cliente S3.

    Variables de entorno requeridas:
        - R2_ENDPOINT_URL
        - R2_ACCESS_KEY_ID
        - R2_SECRET_ACCESS_KEY
        - R2_BUCKET
        - R2_REGION (opcional, por defecto 'auto')

    Returns:
        tuple: (cliente boto3, nombre del bucket).
    """
    load_dotenv()
    endpoint = os.getenv("R2_ENDPOINT_URL")
    access_key = os.getenv("R2_ACCESS_KEY_ID")
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")
    bucket = os.getenv("R2_BUCKET")
    missing = [
        label
        for value, label in (
            (endpoint, "R2_ENDPOINT_URL"),
            (access_key, "R2_ACCESS_KEY_ID"),
            (secret_key, "R2_SECRET_ACCESS_KEY"),
            (bucket, "R2_BUCKET"),
        )
        if not value
    ]
    if missing:
        console.print(
            f"[red]ERROR: Faltan variables en .env: {', '.join(missing)}[/red]"
        )
        raise RuntimeError("Configuración R2 incompleta en .env")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=os.getenv("R2_REGION", "auto"),
    )
    return client, bucket


def list_existing_keys(client, bucket) -> set:
    """
    Lista todos los objetos ya subidos al bucket de R2 (paginado).

    Usa ListObjectsV2, que devuelve hasta 1000 claves por petición, para
    minimizar operaciones y ajustarse al nivel gratuito de R2.

    Args:
        client: Cliente S3 de boto3.
        bucket (str): Nombre del bucket.

    Returns:
        set[str]: Conjunto de claves (nombres de archivo) existentes.
    """
    keys = set()
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            keys.add(obj["Key"])
        if response.get("IsTruncated") and response.get("NextContinuationToken"):
            continuation_token = response["NextContinuationToken"]
        else:
            break
    return keys


def collect_urls_from_source(source_path):
    """
    Recolecta URLs de imágenes desde un archivo CSV o un directorio de CSVs.
    """
    urls = []

    if os.path.isdir(source_path):
        files = [f for f in os.listdir(source_path) if f.lower().endswith(".csv")]
        files = sorted(files, key=csv_sort_key)
        for f in files:
            full_path = os.path.join(source_path, f)
            try:
                with open(full_path, mode="r", encoding="utf-8", newline="") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if "img" in row and row["img"].strip():
                            urls.append(row["img"].strip())
            except Exception as e:
                console.print(f"[red]Error leyendo {f}: {e}[/red]")

    elif os.path.exists(source_path):
        try:
            with open(source_path, mode="r", encoding="utf-8", newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if "img" in row and row["img"].strip():
                        urls.append(row["img"].strip())
        except Exception as e:
            console.print(f"[red]Error leyendo {source_path}: {e}[/red]")

    return list(dict.fromkeys(urls))


async def fetch_optimize_upload(
    session, url: str, key: str, client, bucket: str, retry: int = 0
):
    """
    Descarga una imagen, la convierte a WEBP optimizado y la sube a R2.

    Si ocurre un fallo, se reintenta varias veces con pausas crecientes.

    Args:
        session (aiohttp.ClientSession): Sesión HTTP activa.
        url (str): URL de la imagen a descargar.
        key (str): Clave (nombre de archivo) en el bucket de R2.
        client: Cliente S3 de boto3.
        bucket (str): Nombre del bucket de R2.
        retry (int): Número de reintentos realizados.

    Returns:
        bool: True si la subida fue exitosa, False en caso contrario.
    """
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as resp:
            if resp.status != 200:
                raise Exception(f"HTTP {resp.status}")
            content = await resp.read()

        image = Image.open(BytesIO(content)).convert("RGB")
        buffer = BytesIO()
        image.save(buffer, format="WEBP", optimize=True, quality=IMAGE_QUALITY)

        await asyncio.to_thread(
            client.put_object,
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="image/webp",
            CacheControl=CACHE_CONTROL,
        )
        return True

    except Exception as e:
        if retry < RETRY_LIMIT:
            await asyncio.sleep(3 + retry * 2)
            return await fetch_optimize_upload(
                session, url, key, client, bucket, retry + 1
            )
        console.print(f"[red]Fallo persistente en {url} → {e}[/red]")
        return False


async def upload_all_images_to_r2(csv_source: str):
    """
    Descarga, optimiza y sube a Cloudflare R2 todas las imágenes de cartas de los CSVs.

    Proceso:
        1. Lista las imágenes que ya existen en R2 para no volver a subirlas.
        2. Recopila las URLs únicas de los CSVs.
        3. Descarga cada imagen, la convierte a WEBP optimizado y la sube a R2.
        4. Reintenta las imágenes que fallaron.

    Args:
        csv_source (str): Ruta del archivo CSV o directorio con CSVs.

    Efectos secundarios:
        - Sube las imágenes optimizadas al bucket configurado en .env.
        - Muestra una barra de progreso con el estado del proceso.
    """
    client, bucket = load_r2_client()

    console.print("[cyan]Listando imágenes ya existentes en R2...[/cyan]")
    existing_keys = list_existing_keys(client, bucket)
    console.print(
        f"[cyan]{len(existing_keys)} imágenes encontradas en el bucket '{bucket}'.[/cyan]"
    )

    unique_urls = collect_urls_from_source(csv_source)
    console.print(f"[cyan]{len(unique_urls)} imágenes únicas en los CSVs.[/cyan]")

    to_process = []
    for url in unique_urls:
        name = clean_image_name(url)
        if not name:
            continue
        key = name + WEBP_KEY_EXT
        if key not in existing_keys:
            to_process.append((url, key))

    already_existing = len(unique_urls) - len(to_process)
    console.print(f"[cyan]{already_existing} ya están en R2 (se omiten).[/cyan]")

    uploaded = 0
    failed_urls = []
    sem = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_DOWNLOADS, ssl=False)
    timeout = aiohttp.ClientTimeout(total=40)

    async def worker(url, key):
        nonlocal uploaded
        async with sem:
            ok = await fetch_optimize_upload(session, url, key, client, bucket)
            if ok:
                uploaded += 1
            else:
                failed_urls.append(url)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "{task.completed}/{task.total}",
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Subiendo imágenes a R2", total=len(to_process))
            tasks = [asyncio.create_task(worker(u, k)) for u, k in to_process]
            for i, done in enumerate(asyncio.as_completed(tasks), start=1):
                await done
                progress.advance(task_id)
                if uploaded and i % random.randint(5, 10) == 0:
                    pause = random.randint(3, 8)
                    console.print(f"[yellow]Pausa aleatoria de {pause}s...[/yellow]")
                    await asyncio.sleep(pause)

    if failed_urls:
        console.print(
            f"[magenta]Reintentando {len(failed_urls)} imágenes fallidas...[/magenta]"
        )
        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            for url in failed_urls:
                name = clean_image_name(url)
                if not name:
                    continue
                key = name + WEBP_KEY_EXT
                if await fetch_optimize_upload(session, url, key, client, bucket):
                    uploaded += 1

    console.print(
        f"[green]Subidas: {uploaded} | Ya existentes (omitidas): {already_existing} "
        f"| Pendientes: {len(unique_urls) - already_existing - uploaded}[/green]"
    )


async def download_all_images(csv_source: str):
    """
    Alias de compatibilidad que delega en upload_all_images_to_r2.
    """
    await upload_all_images_to_r2(csv_source)


if __name__ == "__main__":
    """
    Punto de entrada del script.

    Uso:
        python download_images.py archivo.csv
    """
    import sys

    if len(sys.argv) < 2:
        console.print("[yellow]Uso:[/yellow] python download_images.py [archivo.csv]")
        sys.exit(1)

    csv_path = sys.argv[1]
    asyncio.run(upload_all_images_to_r2(csv_path))
