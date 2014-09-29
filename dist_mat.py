#!/usr/bin/python3
"""
Created: 2014/02/28 v1
Revised: 2014/07/25 v2 => command line interface
         2014/09/28 v3 => sqlite3 database incorporation

@author: Ce Gao

distmat read file "addresses.txt" with an address on each line and return the
distance matrix between addrs in a csv file "distmat.csv"

Note there is a query number limit according to google's quota for free service
reference: https://developers.google.com/maps/documentation/distancematrix/

"""

from collections import defaultdict         # addr_book
from itertools import combinations          # for job creating
from time import time, sleep                # timing
import argparse                             # CLI argument parsing
import csv                                  # CSV file output

import requests as rq                       # HTTP query
import xml.etree.ElementTree as et          # Parse the xml


def import_addr(file_name):
    """
    Import address file and return address_book (dictionary)

    Parameters:
    ===========
        file_name: address book file name

    Returns:
    ========
        addr_book: a dict of dict: {group_no: {address_id: address}}
    """

    addr_book = defaultdict(dict) # address book

    id = 0                     # address id
    with open(file_name) as file:
        for line in file:
            # Skip comments and empty line
            if line.startswith('#'): continue
            if line.startswith('\n'): continue

            # Tab separated file
            items = line.rstrip().split('\t')

            add = items[0]      # real address (string)
            grp = int(items[1]) # group number (int)

            addr_book[grp][id] = add
            id += 1

    return addr_book


def create_jobs(addr_book):
    """
    Create query jobs between 2 distinct addresses within the same group, for
    all the batches

    Parameters:
    ===========
        addr_book: dict return by import_addr

    Returns:
    ========
        job_list:  jobs (each job is a tuple: (group_no, ori_id, des_id))
    """

    job_list = []
    for grp, adds in addr_book.items():
        # Address pairs
        id_pairs = combinations(adds.keys(), 2)

        # Append the job
        for id_pair in id_pairs:
            job_list.append((grp, id_pair[0], id_pair[1]))

    return job_list


def query_dist(grp, ori_id, des_id, addr_book):
    """
    Make a (single) request about the distance b/w ori and des

    Parameters:
    ===========
        grp:       group number
        ori_id:    the id of origin address
        des_id:    the id of destination address
        addr_book: dict returned by import_addr

    Returns:
    ========
        dist:      distance between ori and des
    """

    # Get the address string
    ori = addr_book[grp][ori_id]
    des = addr_book[grp][des_id]

    # set request parameter
    url = 'http://maps.googleapis.com/maps/api/distancematrix/xml?'
    para = {'origins'     : ori,
            'destinations': des,
            'sensor'      : 'false'}

    # get request to google maps api
    r = rq.get(url, params=para)

    # parse returned xml
    root = et.fromstring(r.text)
    val = root.find('.//distance//value')

    # return distance
    return val.text


def get_dist_mat(addr_book, job_list):
    """
    Query the distances and construct the distance matrix

    Parameters:
    ===========
        addr_book: address book returned by import_addr
        job_list : job list returned by create_jobs

    Returns:
    ========
        dist_mat: distance matrix, {ori_id: {des_id: distance}}
    """

    dist_mat = defaultdict(dict) # distance matrix

    # Reset the timer
    last_time = 0

    # Job quota
    num_jobs = len(job_list)     # Total number of job
    num_jobs_day = 0             # Number of jobs done today
    max_jobs_day = 2500          # Maximum number of jobs per day

    # Time quota
    time_jobs  = 0.002           # delay time between each job (sec)
    time_quota = 3600            # wait time before reset daily job quota (sec)

    i = 0
    while i < num_jobs:
        # Get current time
        current_time = time()

        # Sleep if the quota of queries per day is exceeded
        if num_jobs_day >= max_jobs_day:
            if current_time - last_time < 3600 * 24:
                sleep(wait_time)
                continue
            else:
                # Reset the timer and start a new day
                num_jobs_day = 0
                last_time = current_time - 0.002

        # Sleep if the time gap between last query is not enough
        if current_time - last_time < time_jobs:
            sleep(time_jobs)

        # Make the query using google maps api
        grp, ori_id, des_id = job_list[i]
        dist = query_dist(grp, ori_id, des_id, addr_book)

        # Reset the time
        last_time = current_time

        # Fill in the distance matrix
        dist_mat[ori_id][des_id] = dist
        dist_mat[des_id][ori_id] = dist

        # Reset job counters
        i += 1
        num_jobs_day += 1

    print('Finish querying!')
    return dist_mat


def export_result(addr_book, dist_mat, file_name):
    """
    Export result to a csv file
    """

    # A dict mapping id to address string
    addr_name = {}

    # Construct addr_name
    for addrs in addr_book.values():
        for id, addr in addrs.items():
            addr_name[id] = addr
    # Get all ids
    addr_ids = sorted(dist_mat.keys())

    # Output the distance matrix into csv files
    with open(file_name, 'w') as csv_file:
        mat_writer = csv.writer(csv_file, delimiter=',')

        # Write header line
        header = ['sample']
        for ori_id in addr_ids:
            header.append(addr_name[ori_id])
        mat_writer.writerow(header)

        # Write remaining lines
        for ori_id in addr_ids:
            line = [addr_name[ori_id]]
            for des_id in addr_ids:
                line.append(dist_mat[ori_id].get(des_id, 0))
            mat_writer.writerow(line)


def main(args):
    """
    main function
    """
    addr_book = import_addr(args.addr)
    job_list  = create_jobs(addr_book)
    dist_mat  = get_dist_mat(addr_book, job_list)
    export_result(addr_book, dist_mat, args.dist)


if __name__ == '__main__':
    des = ('Given an address book, returns the pairwise distances'
           'among addresses by searching Google maps')
    parser = argparse.ArgumentParser(description=des)
    parser.add_argument('addr', help='input address file name')
    parser.add_argument('dist', help='ouptut dist matrix file')

    args = parser.parse_args()

    main(args)
