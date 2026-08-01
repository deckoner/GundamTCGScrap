import asyncio
import os
from rich.console import Console
from csv_scraper import run_scraper
from sql import build_database
from download_images import upload_all_images_to_r2

console = Console()


def main():
    """
    Función principal que coordina todo el flujo de extracción y procesamiento de datos
    del proyecto Gundam GCG.

    Este proceso realiza las siguientes etapas:
      1. Ejecuta el scraper para obtener los datos de todas las cartas desde la web oficial.
      2. Construye una base de datos MariaDB (conectando via .env) con los datos del CSV.
      3. Descarga, optimiza (WEBP) y sube todas las imágenes de las cartas a Cloudflare R2, si el archivo CSV existe.

    Variables:
        csv_name (str): Nombre del archivo CSV de salida.
        use_sqlite (bool): Indica si se debe usar SQLite (True) o MariaDB (False).
        db_name (str): Nombre de la base de datos a crear o actualizar.

    Excepciones:
        Puede lanzar errores de red o escritura si alguno de los pasos falla.
    """
    csv_name = "csv"
    use_sqlite = False
    db_name = "GundamDB"

    # Configuración de pasos a ejecutar (True = ejecutar, False = saltar)
    RUN_SCRAPER = True
    RUN_DB_BUILD = True
    RUN_IMG_DOWNLOAD = True

    if RUN_SCRAPER:
        console.print("[cyan]Iniciando proceso de scraping...[/cyan]")
        run_scraper(csv_name)

    if RUN_DB_BUILD:
        console.print("[cyan]Construyendo base de datos...[/cyan]")
        build_database(csv_name, use_sqlite, db_name)

    if RUN_IMG_DOWNLOAD:
        if os.path.exists(csv_name):
            console.print("[cyan]Descargando, optimizando y subiendo imágenes a R2...[/cyan]")
            asyncio.run(upload_all_images_to_r2(csv_name))
        else:
            console.print(
                f"[red]La carpeta CSV '{csv_name}' no existe, se omite la descarga de imágenes.[/red]"
            )

    console.print(
        "[bold green]Todas las tareas se completaron correctamente.[/bold green]"
    )


if __name__ == "__main__":
    """
    Punto de entrada del script.
    """
    main()
