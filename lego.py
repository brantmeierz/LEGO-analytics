"""


 - CSV datasets from https://www.kaggle.com/datasets/rtatman/lego-database

"""

FORCE_REBUILD = False
""" Remove existing LEGO database and rebuild from CSV."""

import csv
import os
import sqlite3
import lego_data as lego

#from typing import TypedDict

#class Part(TypedDict):
#    name: str
#    category: str
#    part_num: int

def pause() -> None:
    print("Press [Enter] to continue")
    input()

def main():

    # Generate tables if not found or forced
    if os.path.isfile('./lego.db') and FORCE_REBUILD:
        os.remove('./lego.db')
    conn = sqlite3.connect('lego.db')
    if not os.path.isfile('./lego.db'):
        lego.build_tables(conn)

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
            print("\tPrint information about commands")
            print("1) set info (set, s, si)")
            print("\tGet set info by its id")
            print("2) part info (part, p, pi)")
            print("\tGet part info by its id")
            print("3) sets by part (sbp, sets with part, swp)")
            print("\tGet a list of sets containing a specified part and color combination")
            print("4) part search (ps, search part, sp)")
            print("\tSearch for all parts whose names contain a search string")
            pause()
            continue

        elif command in ['set info', 'set', 's', 'si', '1']:
            print("Set ID:")
            set_id = input().strip().lower()
            if not set_id.endswith('-1'):
                set_id = set_id + "-1"
            set_obj = lego.set_by_id(conn, set_id)
            if set_obj is None:
                print("No set found.")
            else:
                lego.print_set(set_obj)
            pause()
            continue

        elif command in ['part info', 'part', 'p', 'pi', '2']:
            print("Part ID:")
            part_id = input().strip().lower()
            part_obj = lego.part_by_id(conn, part_id)
            if part_obj is None:
                print("No part found.")
            else:
                lego.print_part(part_obj)
            pause()
            continue

        elif command in ['sets by part', 'sbp', 'sets with part', 'swp', '3']:
            print("Part ID:")
            part_id = input()
            print("Color Name:")
            color_name = input()
            set_ids = lego.sets_by_part(conn, part_id, color_name)

            if len(set_ids) == 0:
                print("No sets found.")
            else:
                for set_id in set_ids:
                    set_obj = lego.set_by_id(conn, set_id)
                    lego.print_set(set_obj)
                print(str(len(set_ids)) + " total sets.")
            pause()
            continue

        elif command in ['part search', 'ps', 'search part', 'sp', '4']:
            search_again = True
            while search_again:
                print("Word or phrase:")
                name_fragment = input()
                part_ids = lego.parts_by_name(conn, name_fragment)
                if len(part_ids) == 0:
                    print("No parts found.")
                else:
                    for part_id in part_ids:
                        part_obj = lego.part_by_id(conn, part_id)
                        lego.print_part(part_obj)
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

    conn.close()

main()