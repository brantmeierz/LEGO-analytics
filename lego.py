"""


 - CSV datasets from https://www.kaggle.com/datasets/rtatman/lego-database

"""

import csv
import os
import sqlite3


#from typing import TypedDict

#class Part(TypedDict):
#    name: str
#    category: str
#    part_num: int

def create_sets_table(conn: sqlite3.Connection):
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
    with open('./lego_datasets/sets.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [sets] ([set_num], [name], [year], [theme_id], [num_parts])
                VALUES (?, ?, ?, ?, ?)
                """, (row['set_num'], row['name'], row['year'], row['theme_id'], row['num_parts']))
        conn.commit()

def create_parts_table(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [parts] (
            [part_num] TEXT PRIMARY KEY,
            [name] TEXT,
            [part_cat_id] INTEGER
            )
        """)
    conn.commit()
    with open('./lego_datasets/parts.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [parts] ([part_num], [name], [part_cat_id])
                VALUES (?, ?, ?)
                """, (row['part_num'], row['name'], row['part_cat_id']))
        conn.commit()

def create_colors_table(conn: sqlite3.Connection):
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
    with open('./lego_datasets/colors.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [colors] ([id], [name], [rgb], [is_trans])
                VALUES (?, ?, ?, ?)
                """, (row['id'], row['name'], row['rgb'], row['is_trans']))
        conn.commit()

def create_part_categories_table(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [part_categories] (
            [id] INTEGER PRIMARY KEY,
            [name] TEXT
            )
        """)
    conn.commit()
    with open('./lego_datasets/part_categories.csv', encoding="utf8") as csvfile:
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
    with open('./lego_datasets/themes.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [themes] ([id], [name], [parent_id])
                VALUES (?, ?, ?)
                """, (row['id'], row['name'], row['parent_id']))
        conn.commit()

def create_inventory_sets(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [inventory_sets] (
            [inventory_id] INTEGER,
            [set_num] TEXT,
            [quantity] INTEGER,
            PRIMARY KEY ([inventory_id], [set_num], [quantity])
            )
        """)
    conn.commit()
    with open('./lego_datasets/inventory_sets.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [inventory_sets] ([inventory_id], [set_num], [quantity])
                VALUES (?, ?, ?)
                """, (row['inventory_id'], row['set_num'], row['quantity']))
        conn.commit()

def create_inventories(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS [inventories] (
            [id] INTEGER PRIMARY KEY,
            [version] INTEGER,
            [set_num] TEXT
            )
        """)
    conn.commit()
    with open('./lego_datasets/inventories.csv', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            cursor.execute("""
                INSERT INTO [inventories] ([id], [version], [set_num])
                VALUES (?, ?, ?)
                """, (row['id'], row['version'], row['set_num']))
        conn.commit()

def create_inventory_parts(conn: sqlite3.Connection):
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
    with open('./lego_datasets/inventory_parts.csv', encoding="utf8") as csvfile:
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

def set_by_id(conn: sqlite3.Connection, set_id: str) -> dict:
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
    Look up part info for a part id.

    :param conn: The database connection.
    :type sqlite3.Connection:
    :param part_id: The part_id to look up.
    :type str:
    :return: A dictionary representation of the part.
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

def parts_by_name(conn: sqlite3.Connection, name_fragment: str = '') -> list[dict]:
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

def print_set(set_obj):
    print(set_obj['name'] + ' [' + set_obj['set_num'] + '] (' + set_obj['theme'] + ': ' + str(set_obj['year']) + ', ' + str(set_obj['num_parts']) + ' parts)')

def print_part(part_obj):
    print(part_obj['name'] + ' [' + part_obj['part_num'] + '] (' + part_obj['category'] + ')')

def pause() -> None:
    print("Press [Enter] to continue")
    input()

def main():

    conn = sqlite3.connect('lego.db')

    if os.path.isfile('./lego.db'):
        #os.remove('./lego.db')
        pass

    else:
        create_sets_table(conn)
        create_parts_table(conn)
        create_colors_table(conn)
        create_themes_table(conn)
        create_part_categories_table(conn)
        create_inventories(conn)
        create_inventory_sets(conn)
        create_inventory_parts(conn)

    command = ""
    while command not in ['exit', 'quit', 'stop', 'escape']:
        print("================")
        print("Enter a choice:")
        print("0) Help")
        print("1) Set Info")
        print("2) Part Info")
        print("3) Sets By Part")
        print("4) Part Search")
        print("================")

        command = input().strip().lower()

        if command in ['help', 'h', '0']:
            print("Available commands:")
            print("0) help (h)")
            print("1) set info (set, s, si)")
            print("2) part info (part, p, pi)")
            print("3) sets by part (sbp, sets with part, swp)")
            print("4) part search (ps, search part, sp)")
            pause()
            continue

        elif command in ['set info', 'set', 's', 'si', '1']:
            print("Set ID:")
            set_id = input().strip().lower()
            if not set_id.endswith('-1'):
                set_id = set_id + "-1"
            set_obj = set_by_id(conn, set_id)
            if set_obj is None:
                print("No set found.")
            else:
                print_set(set_obj)
            pause()
            continue

        elif command in ['part info', 'part', 'p', 'pi', '2']:
            print("Part ID:")
            part_id = input().strip().lower()
            part_obj = part_by_id(conn, part_id)
            if part_obj is None:
                print("No part found.")
            else:
                print_part(part_obj)
            pause()
            continue

        elif command in ['sets by part', 'sbp', 'sets with part', 'swp', '3']:
            print("Part ID:")
            part_id = input()
            print("Color Name:")
            color_name = input()
            set_ids = sets_by_part(conn, part_id, color_name)

            if len(set_ids) == 0:
                print("No sets found.")
            else:
                for set_id in set_ids:
                    set_obj = set_by_id(conn, set_id)
                    print_set(set_obj)
                print(str(len(set_ids)) + " total sets.")
            pause()
            continue

        elif command in ['part search', 'ps', 'search part', 'sp', '4']:
            search_again = True
            while search_again:
                print("Word or phrase:")
                name_fragment = input()
                part_ids = parts_by_name(conn, name_fragment)
                if len(part_ids) == 0:
                    print("No parts found.")
                else:
                    for part_id in part_ids:
                        part_obj = part_by_id(conn, part_id)
                        print_part(part_obj)
                    print(str(len(part_ids)) + " matching parts.")

                print("Search again? (y/n)")
                choice = input().strip().lower()
                while choice not in ['y', 'n']:
                    print("Search again? (y/n)")
                    choice = input().strip().lower()

                if choice == 'y':
                    continue
                if choice == 'n':
                    search_again = False
            continue

        else:
            print("Unrecognized command. Type 'help' for a list of commands.")
            pause()


    #print(sets_by_part(conn, part_id='6066', color_name='Black'))


    #print(cursor.fetchall())

    conn.close()

main()