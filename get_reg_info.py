#!/usr/bin/env python

""" get_reg_info.py: get registration info for each user associated with one or
    more of a directory of system support maps.

    Usage:
        get_reg_info in_dir out_fname

    Args:
        in_dir: the path for a directory that is expected to contain a set of
        system support maps which have been saved in the ssm database and the
        names for which contain the id of the associated map in the "maps"
        table of the "ssm" database.
        out_fname: the name to be assigned to the .csv output file.
"""

import sys
import os
import collections
import json
import string
import re
import psycopg2
from get_maps_by_version import connect, get_maps

# Indices in "users" and "maps" tables in "ssm" PostgreSQL database:
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
    """ Build a list of integer map ids from a list of ssm file names.

        Arg:
            ssm_list: a list of ssm filenames each of which is expected to have
            the integer id of the associated map as the final character
            substring immediately preceding the ".json" suffix.

        Returns:
            a list of integer ssm ids as they are used in the "maps" table of
            the ssm database.
    """
    ids = []
    for fn in ssm_list:
        ints = map(int, re.findall(r'\d+', fn))
        ids.append(ints[-1:][0])
    return ids


def get_owner_id(map_id, maps):
    """ Get the id of the owner of the ssm with id = map_id

        Args:
            map_id: the id of an ssm in the "maps" table of the "ssm" database.
            maps: a list of tuples for each of which the element in position
            MAPS_ID_IX is expected to be the integer id of an ssm and the
            element in position MAPS_OWNER_ID is expected to be the integer id
            of the owner of tghe ssm.

        Returns:
            the integer id of the owner
   """
    map = [map for map in maps if map[MAPS_ID_IX] == map_id]
    return map[0][MAPS_OWNER_ID]


def get_user_data(user_id, users):
    """ Get all the registration data in the "users" table of the "ssm"
        database for the user with id = user_id.

        Args:
            user_id: the id of the user whose registration data we seek.
            users: a list of user registration data.

        Returns:
            the registration data associated with the user with id = user_id
    """
    user_ix = [i for i, u in enumerate(users) if u[0] == user_id][0]
    user_data = users[user_ix]
    return user_data


def write_headers(outf):
    outf.write("Map ID\tUser ID\tName\tEmail\tState\tReason\n")


def write_owner_data(outf, map_id, owner_data):
    outf.write(str(map_id) + "\t" + str(owner_data[USERS_ID_IX]) + "\t" +
               owner_data[USERS_NAME_IX] + "\t" +
               owner_data[USERS_EMAIL_IX] + "\t" +
               owner_data[USERS_STATE_IX] + "\t" +
               owner_data[USERS_REASON_IX] + "\n")


def main():
    if len(sys.argv) < 3:
        print "usage: get_reg_info.py in_dir out_fname"
        return
    in_dir = sys.argv[1]
    out_fname = sys.argv[2]

    ssm_file_list = get_file_list(in_dir, ".json")
    map_file_ids = get_ssm_ids(ssm_file_list)
    maps = get_maps(MAPS_ID_IX)
    users = get_users(USERS_ID_IX)

    outf = open(out_fname, "w+")
    write_headers(outf)
    for map_id in map_file_ids:
        owner_id = get_owner_id(map_id, maps)
        owner_data = get_user_data(owner_id, users)
        write_owner_data(outf, map_id, owner_data)
    outf.close()

if __name__ == "__main__":
    main()
