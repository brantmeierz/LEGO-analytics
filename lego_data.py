
import csv
from re import I
import sqlite3

def create_sets_table(conn: sqlite3.Connection) -> None:
    """
    Build [sets] table from sets.csv
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [sets] (
            [set_num] TEXT PRIMARY KEY,
            [name] TEXT,
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

def create_parts_table(conn: sqlite3.Connection) -> None:
    """
    Build [parts] table from parts.csv
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [parts] (
            [part_num] TEXT PRIMARY KEY,
            [name] TEXT,
            [part_cat_id] INTEGER
            )
        """)
    conn.commit()
    with open('./lego_data/parts.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [parts] ([part_num], [name], [part_cat_id])
                VALUES (?, ?, ?)
                """, (row['part_num'], row['name'], row['part_cat_id']))
        conn.commit()

def create_colors_table(conn: sqlite3.Connection) -> None:
    """
    Build [colors] table from colors.csv
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [colors] (
            [id] INTEGER PRIMARY KEY,
            [name] TEXT,
            [rgb] TEXT,
            [is_trans] TEXT
            )
        """)
    conn.commit()
    with open('./lego_data/colors.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [colors] ([id], [name], [rgb], [is_trans])
                VALUES (?, ?, ?, ?)
                """, (row['id'], row['name'], row['rgb'], row['is_trans']))
        conn.commit()

def create_part_categories_table(conn: sqlite3.Connection) -> None:
    """
    Build [part_categories] table from part_categories.csv
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [part_categories] (
            [id] INTEGER PRIMARY KEY,
            [name] TEXT
            )
        """)
    conn.commit()
    with open('./lego_data/part_categories.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [part_categories] ([id], [name])
                VALUES (?, ?)
                """, (row['id'], row['name']))
        conn.commit()

def create_themes_table(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [themes] (
            [id] INTEGER PRIMARY KEY,
            [name] TEXT,
            [parent_id] INTEGER
            )
        """)
    conn.commit()
    with open('./lego_data/themes.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [themes] ([id], [name], [parent_id])
                VALUES (?, ?, ?)
                """, (row['id'], row['name'], row['parent_id']))
        conn.commit()

def create_inventory_sets_table(conn: sqlite3.Connection) -> None:
    """
    Build [inventory_sets] table from inventory_sets.csv
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [inventory_sets] (
            [inventory_id] INTEGER,
            [set_num] TEXT,
            [quantity] INTEGER,
            PRIMARY KEY ([inventory_id], [set_num])
            )
        """)
    conn.commit()
    with open('./lego_data/inventory_sets.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [inventory_sets] ([inventory_id], [set_num], [quantity])
                VALUES (?, ?, ?)
                """, (row['inventory_id'], row['set_num'], row['quantity']))
        conn.commit()

def create_inventories_table(conn: sqlite3.Connection) -> None:
    """
    Build [inventories] table from inventories.csv
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [inventories] (
            [id] INTEGER PRIMARY KEY,
            [version] INTEGER,
            [set_num] TEXT
            )
        """)
    conn.commit()
    with open('./lego_data/inventories.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [inventories] ([id], [version], [set_num])
                VALUES (?, ?, ?)
                """, (row['id'], row['version'], row['set_num']))
        conn.commit()

def create_inventory_parts_table(conn: sqlite3.Connection) -> None:
    """
    Build [inventory_parts] table from inventory_parts.csv
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [inventory_parts] (
            [inventory_id] INTEGER,
            [part_num] TEXT,
            [color_id] INTEGER,
            [quantity] INTEGER,
            [is_spare] TEXT
            )
        """)
    conn.commit()
    with open('./lego_data/inventory_parts.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [inventory_parts] ([inventory_id], [part_num], [color_id], [quantity], [is_spare])
                VALUES (?, ?, ?, ?, ?)
                """, (row['inventory_id'], row['part_num'], row['color_id'], row['quantity'], row['is_spare']))
        conn.commit()

def sets_by_part(conn: sqlite3.Connection, part_id: str = '', color_name: str = '', color_exact_match: bool = False) -> list[str]:
    """
    Retrieve a list of all set codes containing the specified part id and/or part color.

    :param conn: Database connection reference.
    :type: sqlite3.Connection
    :param part_id: Part id to search for.
    :type: str
    :param color_name: Part color to search for.
    :type: str
    :param color_exact_match: If True, only return sets that match the color exactly. If False, perform a loose search. Default: False.
    :type: bool
    :return: List of set codes.
    :rtype: list[str]
    """

    if part_id == '' and color_name == '':
        return []

    cursor = conn.cursor()

    where_clause = " WHERE "
    if part_id != '':
        part_id = part_id.strip()
        where_clause += "[inventory_parts].[part_num] = '" +  part_id + "'"
        if color_name != '':
            where_clause += " AND "
    if color_name != '':
        color_name = color_name.strip().lower().replace('grey', 'gray')
        if color_exact_match:
            where_clause += "[colors].[name] = '" + color_name + "'"
        else:
            where_clause += "[colors].[name] LIKE '%" + color_name + "%'"
    where_clause += ' COLLATE NOCASE'

    cursor.execute("""
        SELECT DISTINCT
            [sets].[set_num]
        FROM [sets]
            LEFT JOIN [inventories]
                ON [sets].[set_num] = [inventories].[set_num]
            LEFT JOIN [inventory_parts]
                ON [inventories].[id] = [inventory_parts].[inventory_id]
            LEFT JOIN [colors]
                ON [inventory_parts].[color_id] = [colors].[id]
        """ + where_clause)

    return_list = []
    for result in cursor.fetchall():
        return_list.append(result[0])

    return return_list

def parts_by_set(conn: sqlite3.Connection, set_num = '') -> list[dict]:
    if set_num == '':
        return []

    cursor = conn.cursor()

    return []

def set_by_id(conn: sqlite3.Connection, set_id: str) -> dict:
    """
    Retrieve set data by id.

    :param conn: Database connection reference.
    :type: sqlite3.Connection
    :param set_id: Set id to search for.
    :type: str
    :return: Set data (name, year, theme, num_parts, set_num).
    :rtype: dict
    """

    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT
            [sets].[name] AS 'name',
            [sets].[year] AS 'year',
            [themes].[name] AS 'theme',
            [sets].[num_parts] AS 'num_parts',
            [sets].[set_num] AS 'set_num'
        FROM [sets]
            LEFT JOIN [themes]
                ON [themes].[id] = [sets].[theme_id]
        WHERE [sets].[set_num] = ?
        """,
        [set_id])
    result = cursor.fetchone()
    if result is None:
        return None
    return {
        'name': result[0],
        'year': result[1],
        'theme': result[2],
        'num_parts': result[3],
        'set_num': result[4]
    }

def part_by_id(conn: sqlite3.Connection, part_id: str) -> dict:
    """
    Retrieve part data by id.

    :param conn: The database connection.
    :type sqlite3.Connection:
    :param part_id: The part id to look up.
    :type str:
    :return: Part data
    :rtype dict:
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT
            [parts].[name] AS 'name',
            [part_categories].[name] AS 'category',
            [parts].[part_num] AS 'part_num'
        FROM [parts]
            LEFT JOIN [part_categories]
                ON [parts].[part_cat_id] = [part_categories].[id]
        WHERE [parts].[part_num] = ?
        """,
        [part_id])
    result = cursor.fetchone()
    if result is None:
        return None
    return {
        'name': result[0],
        'category': result[1],
        'part_num': result[2]
    }

def parts_by_name(conn: sqlite3.Connection, name_fragment: str = '') -> list[str]:
    """
    Retrieve a list of parts whose name contains the supplied name fragment.

    :param conn: The database connection.
    :type: sqlite3.Connection
    :param name_fragment: The name fragment to search for.
    :type: str
    :return: A list of part ids.
    :rtype: list[dict]
    """
    if name_fragment == '':
        return []

    name_fragment = '%' + name_fragment.strip().lower() + '%'
    cursor = conn.cursor()
    cursor.execute("""
        SELECT [parts].[part_num]
        FROM [parts]
        WHERE [parts].[name] LIKE ?
        COLLATE NOCASE
        """,
        [name_fragment])

    return_list = []
    for result in cursor.fetchall():
        return_list.append(result[0])

    return return_list

def build_tables(conn: sqlite3.Connection, verbose: bool = False) -> None:
    if verbose:
        print("Creating table [sets]...")
    create_sets_table(conn)
    if verbose:
        print("Creating table [parts]...")
    create_parts_table(conn)
    if verbose:
        print("Creating table [colors]...")
    create_colors_table(conn)
    if verbose:
        print("Creating table [themes]...")
    create_themes_table(conn)
    if verbose:
        print("Creating table [part_categories]...")
    create_part_categories_table(conn)
    if verbose:
        print("Creating table [inventories]...")
    create_inventories_table(conn)
    if verbose:
        print("Creating table [inventory_sets]...")
    create_inventory_sets_table(conn)
    if verbose:
        print("Creating table [inventory_parts]...")
    create_inventory_parts_table(conn)

def print_set(set_obj: dict) -> None:
    print('[' + set_obj['set_num'] + '] ' + set_obj['name'] + '  (' + set_obj['theme'] + ': ' + str(set_obj['year']) + ', ' + str(set_obj['num_parts']) + ' parts)')

def print_part(part_obj: dict) -> None:
    print('[' + part_obj['part_num'] + '] \"' + part_obj['name'] + '\" (' + part_obj['category'] + ')')
