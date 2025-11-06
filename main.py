from rich.console import Console
from csv_scraper import run_scraper
from sql import build_database

console = Console()


def main():
    csv_name = "gundam_cards.csv"
    use_sqlite = False
    db_name = "GundamDB"

    console.print("[cyan]Starting scraper...[/cyan]")
    # run_scraper(csv_name)

    console.print("[cyan]Building database...[/cyan]")
    build_database(csv_name, use_sqlite, db_name)

    console.print("[bold green]All tasks completed successfully[/bold green]")


if __name__ == "__main__":
    main()
