
import csv
import sqlite3

def create_sales_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [sales] (
            [sale_id] INTEGER PRIMARY KEY,
            [date] TEXT,
            [price] REAL,
            [shipping] REAL,
            [seller_name] TEXT,
            [order_number] TEXT,
            [title] TEXT
            )
        """)
    conn.commit()
    with open('./sale_data/sales.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [sales] ([date], [price], [shipping], [seller_name], [order_number], [title])
                VALUES (?, ?, ?, ?, ?, ?)
                """, (row['date'], row['price'], row['shipping'], row['seller_name'], row['order_number'], row['title']))
        conn.commit()

def create_sale_photos_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [sale_photos] (
            [sale_id] INTEGER,
            [photo] TEXT,
            PRIMARY KEY ([sale_id], [photo])
            )
        """)
    conn.commit()
    with open('./sale_data/sale_photos.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [sale_photos] ([sale_id], [photo])
                VALUES (?, ?)
                """, (row['sale_id'], row['photo']))
        conn.commit()

def create_sale_parts_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [sale_parts] (
            [sale_id] INTEGER,
            [part_num] TEXT,
            [color_id] INTEGER,
            [quantity] INTEGER
            )""")
    conn.commit()
    with open('./sale_data/sale_parts.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [sale_parts] ([sale_id], [part_num], [color_id], [quantity])
                VALUES (?, ?, ?, ?)
                """, (row['sale_id'], row['part_num'], row['color_id'], row['quantity']))
        conn.commit()

def get_sale_data(conn: sqlite3.Connection, sale_id: int) -> dict:
    cursor = conn.cursor()
    sale_record = {}
    cursor.execute("""
        SELECT
            [title],
            [price],
            [shipping],
            [seller_name]
        FROM [sales]
        WHERE [sale_id] = ?
        """, [sale_id])
    sale_data = cursor.fetchone()
    sale_record['title'] = sale_data[0]
    sale_record['price'] = sale_data[1]
    sale_record['shipping'] = sale_data[2]
    sale_record['seller'] = sale_data[3]
    cursor.execute("""
        SELECT
            [photo]
        FROM [sale_photos]
        WHERE [sale_id] = ?
        """, [sale_id])
    sale_photos = cursor.fetchall()
    photo_list = []
    for photo in sale_photos:
        photo_list.append(photo[0])
    sale_record['photos'] = photo_list

    return sale_record

def print_sale(sale_obj: dict) -> None:
    print("($" + str(sale_obj['price'] + sale_obj['shipping']) + ") " + sale_obj['title'] + " from seller " + sale_obj['seller'])

def build_tables(conn: sqlite3.Connection, verbose: bool = False) -> None:
    if verbose:
        print("Creating table [sales]...")
    create_sales_table(conn)
    if verbose:
        print("Creating table [sale_photos]...")
    create_sale_photos_table(conn)
    if verbose:
        print("Creating table [sale_parts]...")
    create_sale_parts_table(conn)