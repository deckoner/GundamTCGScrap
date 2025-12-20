import asyncio
import os
from rich.console import Console
from csv_scraper import run_scraper
from sql import build_database
from download_images import download_all_images

console = Console()


def main():
    """
    Función principal que coordina todo el flujo de extracción y procesamiento de datos
    del proyecto Gundam GCG.

    Este proceso realiza las siguientes etapas:
      1. Ejecuta el scraper para obtener los datos de todas las cartas desde la web oficial.
      2. Construye una base de datos (SQLite o SQL Server, según configuración) con los datos del CSV.
      3. Descarga y optimiza todas las imágenes de las cartas, si el archivo CSV existe.

    Variables:
        csv_name (str): Nombre del archivo CSV de salida.
        use_sqlite (bool): Indica si se debe usar SQLite (True) o SQL Server (False).
        db_name (str): Nombre de la base de datos a crear o actualizar.

    Excepciones:
        Puede lanzar errores de red o escritura si alguno de los pasos falla.
    """
    csv_name = "csv"
    use_sqlite = False
    db_name = "GundamDB"

    # Configuración de pasos a ejecutar (True = ejecutar, False = saltar)
    RUN_SCRAPER = True
    RUN_DB_BUILD = False
    RUN_IMG_DOWNLOAD = False

    if RUN_SCRAPER:
        console.print("[cyan]Iniciando proceso de scraping...[/cyan]")
        run_scraper(csv_name)

    if RUN_DB_BUILD:
        console.print("[cyan]Construyendo base de datos...[/cyan]")
        build_database(csv_name, use_sqlite, db_name)

    if RUN_IMG_DOWNLOAD:
        if os.path.exists(csv_name):
            console.print("[cyan]Descargando y optimizando imágenes...[/cyan]")
            asyncio.run(download_all_images(csv_name))
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
