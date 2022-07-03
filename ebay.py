
import csv
import sqlite3

def create_sales_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [sales] (
            [sale_id] INTEGER PRIMARY KEY,
            [date] TEXT,
            [year] INTEGER,
            [theme_id] INTEGER,
            [num_parts] INTEGER
            )
        """)
    conn.commit()
    with open('./lego_data/sets.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [sets] ([set_num], [name], [year], [theme_id], [num_parts])
                VALUES (?, ?, ?, ?, ?)
                """, (row['set_num'], row['name'], row['year'], row['theme_id'], row['num_parts']))
        conn.commit()

    pass