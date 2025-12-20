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
    Divide un texto que contiene múltiples valores separados por delimitadores comunes
    (como comas, barras, punto y coma, o 'y') en una lista de valores limpios y únicos.

    Args:
        s (str | int | float): Texto o número a procesar.

    Returns:
        list[str]: Lista de valores únicos, sin duplicados ni entradas vacías.
    """
    if pd.isna(s) or s == "":
        return []
    if isinstance(s, (int, float)):
        s = str(s)
    s = s.strip()
    parts = re.split(r"[\/;,|]+|\s{2,}|\s+y\s+", s)
    cleaned = [
        p.strip() for p in parts if p.strip() and p not in ("-", "NULL", "null", "None")
    ]
    return list(dict.fromkeys(cleaned))


def split_traits(s):
    """
    Procesa la columna 'trait' de una carta para extraer los rasgos (traits)
    de forma segura y consistente, conservando los nombres compuestos
    y eliminando texto residual.

    Maneja formatos como:
        - 'Earth Federation y White Base Team'
        - '(White Base Team) Trait'
        - 'NULL', 'None' o '-'

    Args:
        s (str): Texto de entrada con los traits.

    Returns:
        list[str]: Lista de traits únicos y limpios.
    """
    if pd.isna(s) or str(s).strip() == "":
        return []
    raw = str(s).strip()

    if raw.upper() in ("NULL", "NONE", "-"):
        return []

    paren = re.findall(r"\(([^)]+)\)", raw)
    text_no_paren = re.sub(r"\([^)]+\)", " ", raw)
    normalized = re.sub(r"[\/;|\u3001]", "|||", text_no_paren)
    normalized = re.sub(r"\s+y\s+", "|||", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s{2,}", "|||", normalized)
    parts = [p.strip() for p in normalized.split("|||") if p.strip()]
    combined = paren + parts

    clean = []
    for p in combined:
        p2 = re.sub(r"\bTrait\b", "", p, flags=re.IGNORECASE).strip()
        p2 = p2.strip("[] ,;:（）()")
        if p2 and p2.upper() not in ("NULL", "NONE", "-"):
            clean.append(p2)
    return list(dict.fromkeys(clean))


def extract_tags_from_text(text):
    """
    Extrae etiquetas (tags) del texto de una carta, buscando patrones entre
    los delimitadores '<...>' y '【...】'.

    Args:
        text (str): Texto de la carta.

    Returns:
        list[str]: Lista de etiquetas únicas encontradas.
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
    Convierte un valor a entero de forma segura, devolviendo None si no es posible.

    Args:
        x (any): Valor a convertir.

    Returns:
        int | None: Entero convertido o None si no es válido.
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
    Establece una conexión con una base de datos SQLite.

    Args:
        db (str): Nombre del archivo .sqlite.

    Returns:
        sqlite3.Connection: Conexión abierta a la base de datos.
    """
    return sqlite3.connect(db)


def connect_mariadb(db):
    """
    Conecta con una base de datos MariaDB utilizando las credenciales del archivo .env.

    Variables de entorno requeridas:
        - DB_HOST
        - DB_USER
        - DB_PASSWORD

    Args:
        db (str): Nombre de la base de datos.

    Returns:
        pymysql.Connection: Conexión a la base de datos MariaDB.
    """
    load_dotenv()
    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    if not db:
        console.print("[red]ERROR: Falta DB_NAME en el archivo .env[/red]")
        raise RuntimeError("DB_NAME no definido en .env")
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
    """
    Crea el esquema de tablas para la base de datos (SQLite o MariaDB).

    Incluye:
        - Tablas principales de cartas y atributos (traits, colors, types, etc.)
        - Tablas de relación (card_traits, card_colors, etc.)
        - Tablas de usuario, colección y mazos.

    Args:
        conn: Conexión activa a la base de datos.
        maria (bool): True si se usa MariaDB, False para SQLite.
    """
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
            tag_ids TEXT,
            trait_ids TEXT
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
            tag_ids TEXT,
            trait_ids TEXT
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
            ("card_traits", "card_id", "trait_id"),
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
    """
    Obtiene el ID de un valor existente en una tabla o lo crea si no existe.

    Args:
        conn: Conexión a la base de datos.
        table (str): Nombre de la tabla.
        col (str): Nombre de la columna.
        value (str): Valor a buscar o insertar.
        maria (bool): True si se usa MariaDB.

    Returns:
        int | None: ID del registro correspondiente.
    """
    if not value:
        return None
    cur = conn.cursor()
    ph = "%s" if maria else "?"
    cur.execute(f"SELECT id FROM {table} WHERE {col}={ph}", (value,))
    row = cur.fetchone()
    if row:
        try:
            return row[0]
        except Exception:
            return row
    if maria:
        cur.execute(f"INSERT IGNORE INTO {table}({col}) VALUES ({ph})", (value,))
        cur.execute(f"SELECT id FROM {table} WHERE {col}=%s", (value,))
        row2 = cur.fetchone()
        return row2[0] if row2 else None
    else:
        cur.execute(f"INSERT OR IGNORE INTO {table}({col}) VALUES ({ph})", (value,))
        conn.commit()
        return cur.lastrowid


def process_csv_to_db(conn, csv_path, maria=False):
    """
    Importa los datos del archivo CSV al esquema de la base de datos.

    Este proceso:
      - Lee el CSV generado por el scraper.
      - Limpia y normaliza cada campo.
      - Inserta cartas, atributos y relaciones en sus respectivas tablas.
      - Establece las claves foráneas y relaciones N:M (traits, tags, etc.)

    Args:
        conn: Conexión activa a la base de datos.
        csv_path (str): Ruta al archivo CSV.
        maria (bool): True si se usa MariaDB.
    """
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    placeholder = "%s" if maria else "?"
    cur = conn.cursor()
    for _, r in track(df.iterrows(), total=len(df)):
        rarity = (
            r["rarity"].strip() if "rarity" in r and r["rarity"] is not None else ""
        )
        zone_id = get_or_create(conn, "zones", "zone", r["zone"].strip(), maria)
        link_id = get_or_create(conn, "links", "link", r["link"].strip(), maria)
        anime_id = get_or_create(conn, "animes", "anime", r["anime"].strip(), maria)
        belongs_gd_id = get_or_create(
            conn, "belongs_gd", "belongs_gd", r["belongs_gd"].strip(), maria
        )

        colors = split_multi_values(r.get("color", ""))
        types = split_multi_values(r.get("type", ""))
        traits = split_traits(r.get("trait", ""))
        tags = extract_tags_from_text(r.get("text_card", ""))

        color_ids = [get_or_create(conn, "colors", "color", c, maria) for c in colors]
        type_ids = [get_or_create(conn, "types", "type", t, maria) for t in types]
        tag_ids = [get_or_create(conn, "tags", "tag", t, maria) for t in tags]
        trait_ids = [get_or_create(conn, "traits", "trait", t, maria) for t in traits]

        img_url = r.get("img", "").strip()
        img_name = ""
        if img_url:
            base = os.path.basename(img_url)
            base = base.split(".webp")[0]
            img_name = base
        
        alt_art = bool(re.search(r"_.+", img_name))
        cols = "gd,name,rarity,level,cost,text_card,zone_id,link_id,ap,hp,anime_id,belongs_gd_id,img,alt_art,color_ids,type_ids,tag_ids,trait_ids"
        placeholders = ",".join([placeholder] * 18)
        if maria:
            sql = f"INSERT IGNORE INTO cards ({cols}) VALUES ({placeholders})"
        else:
            sql = f"INSERT OR IGNORE INTO cards ({cols}) VALUES ({placeholders})"

        params = (
            r.get("GD", "").strip(),
            r.get("name", "").strip(),
            rarity,
            safe_int(r.get("level", "")),
            safe_int(r.get("cost", "")),
            r.get("text_card", "").strip(),
            zone_id,
            link_id,
            r.get("ap", "").strip(),
            r.get("hp", "").strip(),
            anime_id,
            belongs_gd_id,
            img_name,
            alt_art,
            ",".join(map(str, color_ids)),
            ",".join(map(str, type_ids)),
            ",".join(map(str, tag_ids)),
            ",".join(map(str, trait_ids)),
        )
        cur.execute(sql, params)
        card_id = getattr(cur, "lastrowid", None)
        if not card_id:
            try:
                cur.execute(
                    (
                        "SELECT id FROM cards WHERE gd=%s"
                        if maria
                        else "SELECT id FROM cards WHERE gd=?"
                    ),
                    (r.get("GD", "").strip(),),
                )
                rr = cur.fetchone()
                card_id = rr[0] if rr else None
            except Exception:
                card_id = None

        for table, ids in [
            ("card_colors", color_ids),
            ("card_types", type_ids),
            ("card_tags", tag_ids),
            ("card_traits", trait_ids),
        ]:
            for tid in ids:
                if tid is None:
                    continue
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


def build_database(csv_source, use_sqlite=True, db_name="GundamDB"):
    """
    Construye una base de datos completa a partir de archivos CSV.

    Este proceso:
        1. Conecta a la base de datos (SQLite o MariaDB segun el parametro `use_sqlite`).
        2. Crea todas las tablas necesarias mediante `create_schema()`.
        3. Si `csv_source` es un directorio, importa todos los .csv encontrados.
           Si es un archivo, importa ese unico archivo.
        4. Cierra la conexión y confirma la finalización.

    Args:
        csv_source (str): Ruta al archivo CSV o directorio con CSVs.
        use_sqlite (bool, opcional): Si es True, usa SQLite (modo local).
            Si es False, usa MariaDB con las credenciales definidas en `.env`.
        db_name (str, opcional): Nombre de la base de datos (sin extensión .sqlite).
    """
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
    
    if os.path.isdir(csv_source):
        files = [f for f in os.listdir(csv_source) if f.lower().endswith(".csv")]
        console.print(f"[cyan]Found {len(files)} CSV files in '{csv_source}'[/cyan]")
        for f in files:
            full_path = os.path.join(csv_source, f)
            console.print(f"[cyan]Processing {f}...[/cyan]")
            process_csv_to_db(conn, full_path, maria=maria)
    else:
        if os.path.exists(csv_source):
            process_csv_to_db(conn, csv_source, maria=maria)
        else:
             console.print(f"[red]csv_source '{csv_source}' not found.[/red]")

    conn.close()
    console.print(f"[green]Database '{db_name}' ready.[/green]")
