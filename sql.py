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
    return sqlite3.connect(db)


def connect_mariadb(db):
    load_dotenv()
    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    if not db:
        console.print("[red]ERROR: Missing DB_NAME in .env file[/red]")
        raise RuntimeError("DB_NAME not defined in .env")
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=db,
        autocommit=True,
        charset="utf8mb4",
    )
    return connection


def create_schema(conn, maria=False):
    cur = conn.cursor()
    if not maria:
        cur.executescript(
            """
        PRAGMA foreign_keys = ON;
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

        CREATE TABLE IF NOT EXISTS traits(id INTEGER PRIMARY KEY AUTOINCREMENT, trait TEXT UNIQUE);
        CREATE TABLE IF NOT EXISTS card_traits(card_id INT, trait_id INT, PRIMARY KEY(card_id,trait_id));

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
        cur.execute(
            "CREATE TABLE IF NOT EXISTS traits(id INT AUTO_INCREMENT PRIMARY KEY, trait VARCHAR(255) UNIQUE) ENGINE=InnoDB;"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS card_traits(card_id INT, trait_id INT, PRIMARY KEY(card_id,trait_id)) ENGINE=InnoDB;"
        )
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
        for tbl, a, b in [
            ("card_colors", "card_id", "color_id"),
            ("card_types", "card_id", "type_id"),
            ("card_tags", "card_id", "tag_id"),
        ]:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {tbl}({a} INT, {b} INT, PRIMARY KEY({a},{b})) ENGINE=InnoDB;"
            )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS users(id INT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(100) UNIQUE NOT NULL, password_hash TEXT) ENGINE=InnoDB;"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS user_collections(user_id INT, card_id INT, quantity INT DEFAULT 1, PRIMARY KEY(user_id, card_id), FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE, FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE) ENGINE=InnoDB;"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS decks(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, user_id INT, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE) ENGINE=InnoDB;"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS deck_cards(deck_id INT, card_id INT, quantity INT DEFAULT 1, PRIMARY KEY(deck_id, card_id), FOREIGN KEY(deck_id) REFERENCES decks(id) ON DELETE CASCADE, FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE) ENGINE=InnoDB;"
        )


def get_or_create(conn, table, col, value, maria=False):
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
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    placeholder = "%s" if maria else "?"
    cur = conn.cursor()
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
        traits = split_multi_values(r.get("trait", ""))
        color_ids = [get_or_create(conn, "colors", "color", c, maria) for c in colors]
        type_ids = [get_or_create(conn, "types", "type", t, maria) for t in types]
        tag_ids = [get_or_create(conn, "tags", "tag", t, maria) for t in tags]
        trait_ids = [get_or_create(conn, "traits", "trait", t, maria) for t in traits]
        img_url = r["img"].strip()
        img_name = ""
        if img_url:
            base = os.path.basename(img_url)
            base = base.split(".webp")[0]
            img_name = base
        sql = f"INSERT OR IGNORE INTO cards (gd,name,rarity,level,cost,text_card,zone_id,link_id,ap,hp,anime_id,belongs_gd_id,img,alt_art,color_ids,type_ids,tag_ids) VALUES ({','.join([placeholder]*17)})"
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
            img_name,
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
            ("card_traits", trait_ids),
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


def build_database(csv_path, use_sqlite=True, db_name="GundamDB"):
    console.print(
        f"[cyan]Connecting to {'SQLite' if use_sqlite else 'MariaDB'} database...[/cyan]"
    )

    if use_sqlite:
        conn = connect_sqlite(db_name + ".sqlite")
        maria = False
    else:
        conn = connect_mariadb(db_name)
        maria = True

    console.print("[cyan]Creating database schema...[/cyan]")
    create_schema(conn, maria=maria)

    console.print("[cyan]Importing CSV data into database...[/cyan]")
    process_csv_to_db(conn, csv_path, maria=maria)

    conn.close()
    console.print(f"[green]Database '{db_name}' ready.[/green]")
