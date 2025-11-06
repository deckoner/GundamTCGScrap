import os
import re
import sqlite3
import pandas as pd
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import track

console = Console()

try:
    import pymysql
except ImportError:
    pymysql = None


def split_multi_values(s):
    """
    Splits a string containing multiple values separated by common delimiters.
    Removes duplicates and ignores invalid or placeholder values.
    Returns a list of cleaned unique strings.
    """
    if pd.isna(s) or s == "":
        return []
    if isinstance(s, (int, float)):
        s = str(s)

    parts = re.split(r"[\/;,|]+|\s{2,}|(?<=\w)\s(?=\w)|,", s)
    cleaned = [
        p.strip() for p in parts if p.strip() and p not in ("-", "NULL", "null", "None")
    ]
    return list(dict.fromkeys(cleaned))


def extract_tags_from_text(text):
    """
    Extracts tag values that appear inside <...> or 【...】 markers.
    Normalizes whitespace and removes duplicates.
    """
    if pd.isna(text) or not isinstance(text, str):
        return []

    tags = re.findall(r"<([^>]+)>", text) + re.findall(r"【([^】]+)】", text)
    clean = []
    for t in tags:
        t = " ".join(t.strip().split())
        if t and t not in clean:
            clean.append(t)
    return clean


def safe_int(x):
    """
    Converts a value to an integer when possible.
    Returns None if value is empty or conversion is not possible.
    """
    try:
        if pd.isna(x) or x == "":
            return None
        return int(x)
    except:
        try:
            return int(float(x))
        except:
            return None


def connect_sqlite(db):
    """
    Creates and returns a SQLite connection to the provided database file.
    """
    return sqlite3.connect(db)


def connect_mariadb(db):
    """
    Creates and returns a connection to MariaDB using environment variables.
    Requires pymysql and python-dotenv installed.
    """
    load_dotenv()

    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")

    if not db:
        console.print("[red]ERROR: Missing DB_NAME in .env file[/red]")
        raise RuntimeError("DB_NAME not defined in .env")

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db,
            autocommit=True,
            charset="utf8mb4",
        )
        console.print("[green]MariaDB connection established successfully[/green]")
        return connection

    except pymysql.MySQLError as e:
        console.print(f"[red]Error connecting to MariaDB:[/red] {e}")
        raise


def create_schema(conn, maria=False):
    """
    Creates all database tables required for the card system and user management.
    Supports both SQLite and MariaDB schema definitions.
    Includes users, user collections, and decks (with ownership and cards).
    """
    cur = conn.cursor()

    if not maria:
        cur.executescript(
            """
        PRAGMA foreign_keys = ON;

        -- Base card data
        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gd TEXT,
            name TEXT,
            rarity TEXT,
            level INT,
            cost INT,
            text_card TEXT,
            zone_id INT,
            link_id INT,
            anime_id INT,
            belongs_gd_id INT,
            ap TEXT,
            hp TEXT,
            img TEXT,
            alt_art BOOLEAN,
            color_ids TEXT,
            type_ids TEXT,
            tag_ids TEXT
        );

        CREATE TABLE IF NOT EXISTS colors(id INTEGER PRIMARY KEY AUTOINCREMENT, color TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS types(id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS tags(id INTEGER PRIMARY KEY AUTOINCREMENT, tag TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS zones(id INTEGER PRIMARY KEY AUTOINCREMENT, zone TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS links(id INTEGER PRIMARY KEY AUTOINCREMENT, link TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS animes(id INTEGER PRIMARY KEY AUTOINCREMENT, anime TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS belongs_gd(id INTEGER PRIMARY KEY AUTOINCREMENT, belongs_gd TEXT UNIQUE);

        CREATE TABLE IF NOT EXISTS card_colors(card_id INT, color_id INT, PRIMARY KEY(card_id,color_id));
        CREATE TABLE IF NOT EXISTS card_types(card_id INT, type_id INT, PRIMARY KEY(card_id,type_id));
        CREATE TABLE IF NOT EXISTS card_tags(card_id INT, tag_id INT, PRIMARY KEY(card_id,tag_id));

        -- User and deck system
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS user_collections (
            user_id INT,
            card_id INT,
            quantity INT DEFAULT 1,
            PRIMARY KEY(user_id, card_id),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS decks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            user_id INT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS deck_cards (
            deck_id INT,
            card_id INT,
            quantity INT DEFAULT 1,
            PRIMARY KEY(deck_id, card_id),
            FOREIGN KEY(deck_id) REFERENCES decks(id) ON DELETE CASCADE,
            FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
        );
        """
        )
    else:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS cards (
            id INT AUTO_INCREMENT PRIMARY KEY,
            gd VARCHAR(50),
            name TEXT,
            rarity VARCHAR(50),
            level INT,
            cost INT,
            text_card TEXT,
            zone_id INT,
            link_id INT,
            anime_id INT,
            belongs_gd_id INT,
            ap TEXT,
            hp TEXT,
            img TEXT,
            alt_art BOOLEAN,
            color_ids TEXT,
            type_ids TEXT,
            tag_ids TEXT
        ) ENGINE=InnoDB;
        """
        )

        # Lookup tables
        for tbl, col in [
            ("colors", "color"),
            ("types", "type"),
            ("tags", "tag"),
            ("zones", "zone"),
            ("links", "link"),
            ("animes", "anime"),
            ("belongs_gd", "belongs_gd"),
        ]:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {tbl}(id INT AUTO_INCREMENT PRIMARY KEY,{col} VARCHAR(255) UNIQUE) ENGINE=InnoDB;"
            )

        # Relationship tables
        for tbl, a, b in [
            ("card_colors", "card_id", "color_id"),
            ("card_types", "card_id", "type_id"),
            ("card_tags", "card_id", "tag_id"),
        ]:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {tbl}({a} INT, {b} INT, PRIMARY KEY({a},{b})) ENGINE=InnoDB;"
            )

        # User and deck tables
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(100) UNIQUE NOT NULL,
            password_hash TEXT
        ) ENGINE=InnoDB;
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS user_collections (
            user_id INT,
            card_id INT,
            quantity INT DEFAULT 1,
            PRIMARY KEY(user_id, card_id),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
        ) ENGINE=InnoDB;
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS decks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            user_id INT,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        ) ENGINE=InnoDB;
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS deck_cards (
            deck_id INT,
            card_id INT,
            quantity INT DEFAULT 1,
            PRIMARY KEY(deck_id, card_id),
            FOREIGN KEY(deck_id) REFERENCES decks(id) ON DELETE CASCADE,
            FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
        ) ENGINE=InnoDB;
        """
        )


def get_or_create(conn, table, col, value, maria=False):
    """
    Inserts a value into a lookup table if it does not exist.
    Returns the corresponding row id.
    Supports both SQLite and MariaDB.
    """
    if not value:
        return None

    cur = conn.cursor()
    ph = "%s" if maria else "?"

    cur.execute(f"SELECT id FROM {table} WHERE {col}={ph}", (value,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(f"INSERT INTO {table}({col}) VALUES ({ph})", (value,))
    if not maria:
        conn.commit()

    return cur.lastrowid


def process_csv_to_db(conn, csv_path, maria=False):
    """
    Imports card data from a CSV file into the database.
    Creates relationships for colors, types, and tags.
    Handles both SQLite and MariaDB insert syntax.
    """
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    placeholder = "%s" if maria else "?"
    cur = conn.cursor()

    console.print("[cyan]Importing cards...[/cyan]")

    for _, r in track(df.iterrows(), total=len(df)):
        rarity = r["rarity"].strip()
        alt_art = "+" in rarity

        zone_id = get_or_create(conn, "zones", "zone", r["zone"].strip(), maria)
        link_id = get_or_create(conn, "links", "link", r["link"].strip(), maria)
        anime_id = get_or_create(conn, "animes", "anime", r["anime"].strip(), maria)
        belongs_gd_id = get_or_create(
            conn, "belongs_gd", "belongs_gd", r["belongs_gd"].strip(), maria
        )

        colors = split_multi_values(r["color"])
        types = split_multi_values(r["type"])
        tags = extract_tags_from_text(r["text_card"])

        color_ids = [get_or_create(conn, "colors", "color", c, maria) for c in colors]
        type_ids = [get_or_create(conn, "types", "type", t, maria) for t in types]
        tag_ids = [get_or_create(conn, "tags", "tag", t, maria) for t in tags]

        sql = (
            f"INSERT OR IGNORE INTO cards ("
            "gd,name,rarity,level,cost,text_card,zone_id,link_id,ap,hp,anime_id,belongs_gd_id,img,"
            "alt_art,color_ids,type_ids,tag_ids"
            f") VALUES ({','.join([placeholder]*17)})"
            if not maria
            else f"INSERT IGNORE INTO cards ("
            "gd,name,rarity,level,cost,text_card,zone_id,link_id,ap,hp,anime_id,belongs_gd_id,img,"
            "alt_art,color_ids,type_ids,tag_ids"
            f") VALUES ({','.join([placeholder]*17)})"
        )

        params = (
            r["GD"].strip(),
            r["name"].strip(),
            rarity,
            safe_int(r["level"]),
            safe_int(r["cost"]),
            r["text_card"].strip(),
            zone_id,
            link_id,
            r["ap"].strip(),
            r["hp"].strip(),
            anime_id,
            belongs_gd_id,
            r["img"].strip(),
            alt_art,
            ",".join(map(str, color_ids)),
            ",".join(map(str, type_ids)),
            ",".join(map(str, tag_ids)),
        )

        cur.execute(sql, params)
        card_id = cur.lastrowid

        for table, ids in [
            ("card_colors", color_ids),
            ("card_types", type_ids),
            ("card_tags", tag_ids),
        ]:
            for tid in ids:
                if maria:
                    cur.execute(
                        f"INSERT IGNORE INTO {table} VALUES ({placeholder},{placeholder})",
                        (card_id, tid),
                    )
                else:
                    cur.execute(
                        f"INSERT OR IGNORE INTO {table} VALUES ({placeholder},{placeholder})",
                        (card_id, tid),
                    )

    conn.commit()
    console.print("[green]Import completed[/green]")


def build_database(csv, use_sqlite=True, db=None):
    """
    Creates and populates a database from a CSV file.
    Allows switching between SQLite and MariaDB backend.
    """
    console.print("[magenta]Creating database...[/magenta]")
    db = db or ("cards.db" if use_sqlite else "cards")
    conn = connect_sqlite(db) if use_sqlite else connect_mariadb(db)
    create_schema(conn, maria=not use_sqlite)
    process_csv_to_db(conn, csv, maria=not use_sqlite)
    conn.close()
    console.print(f"[green]Database ready: {db}[/green]")


def update_database(csv, use_sqlite=True, db=None):
    """
    Rebuilds and refreshes the database using a new CSV file.
    Existing data is replaced.
    """
    console.print("[yellow]Updating database...[/yellow]")
    build_database(csv, use_sqlite, db)
