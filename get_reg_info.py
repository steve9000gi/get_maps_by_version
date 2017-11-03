#!/usr/bin/env python

""" get_reg_info.py: get registration info for each user associated with a directory
    of system support maps.

    Usage:
        get_reg_info in_dir out_dir

"""

import sys
import os
import collections
import json
import string
import re
import psycopg2
from get_maps_by_version import connect, get_maps

# Indices in "users" and "maps" tables in "ssm" Postgresql database:
USERS_ID_IX = 0
USERS_EMAIL_IX = 1
USERS_NAME_IX = 5
USERS_STATE_IX = 6
USERS_REASON_IX = 13
MAPS_ID_IX = 0
MAPS_OWNER_ID = 1

def get_users(sort_index):
    """ Get a list of tuples from the PostgreSQL ssm database, each tuple of
        which is a dict representing a registered ssm user.

        Arg:
            sort_index: the int index of the tuple elements to be used as the
            basis for sorting the tuples in the list.

        Returns:
            a sorted list of tuples.
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT * from users")
    users = cur.fetchall()
    cur.close()
    conn.close()
    return sorted(users, key=lambda k: k[sort_index])


def get_file_list(dir, suffix):
    """ Get a list of all the files (in "dir") whose names end in "suffix."
        Note: copied and pasted from CMs_to_3cols.py. 2do: Tighten this up.
    Args:
        dir: the path to a directory.
        suffix: the ending substring used for selecting files.
    Returns:
        a list of files in "dir" ending with "suffix."
    """
    files = []
    files += [fn for fn in os.listdir(dir) if fn.endswith(suffix)]
    return files


def get_ssm_ids(ssm_list):
    """ build a list of integer map ids from a list of ssm file names.

    """
    ids = []
    for fn in ssm_list:
        ints = map(int, re.findall(r'\d+', fn))
        ids.append(ints[-1:][0])
    return ids


def get_owner_id(map_id, maps):
    map = [map for map in maps if map[MAPS_ID_IX] == map_id] 
    return map[0][MAPS_OWNER_ID]


def get_owner_data(owner_id, owners):
    owner_ix = [i for i, u in enumerate(owners) if u[0] == owner_id][0]
    owner_data = owners[owner_ix]
    return owner_data;


def print_headers():
    print("Map ID\tUser ID\tName\tEmail\tState\tReason for Creating SSM")


def print_owner_data(mi, od):
    print (str(mi) + "\t" + str(od[USERS_ID_IX]) + "\t" + od[USERS_NAME_IX] +
           "\t" + od[USERS_EMAIL_IX] + "\t" + od[USERS_STATE_IX] + "\t" +
           od[USERS_REASON_IX])


def main():

    if len(sys.argv) < 3:
        print "usage: get_reg_info.py in_dir out_dir"
        return
    in_dir = sys.argv[1]
    out_dir = sys.argv[2]
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        print "Created " + out_dir

    ssm_file_list = get_file_list(in_dir, ".json")
    map_file_ids = get_ssm_ids(ssm_file_list)
    maps = get_maps(MAPS_ID_IX)
    users = get_users(USERS_ID_IX)

    print_headers()
    for map_id in map_file_ids:
        owner_id = get_owner_id(map_id, maps)
        owner_data = get_owner_data(owner_id, users)
        print_owner_data(map_id, owner_data)

if __name__ == "__main__":
    main()

