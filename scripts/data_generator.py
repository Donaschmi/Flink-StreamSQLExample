#! /usr/bin/python3
from random import randint
import argparse
import pathlib
import getopt
import sys


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "k:s:m:", ["data_dir=", "keys=", "key_matching=", "size="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(err)  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    tables = 2
    keys = 10
    key_matching = 0.5
    dataset_size = 1 # In Gigabytes
    data_dir = "/home/donatien/GEPICIAD/resource-estimator/sql_illustration/data/{}Gb/tables/"

    for o, a in opts:
        if o == "--data_dir":
            data_dir = a
        elif o in ("-k", "--keys"):
            keys = int(a)
        elif o in ("-m", "--key_matching"):
            key_matching = float(a)
        elif o in ("-s", "--size"):
            dataset_size = float(a)
        else:
            assert False, "unhandled option"

    data_dir = data_dir.format(dataset_size)
    pathlib.Path(data_dir).mkdir(exist_ok=True, parents=True)
    table1_row = "{}|AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
    table2_row = "{}|BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\n"
    row_size = 100
    size_per_table = dataset_size * 1073741824 # in bytes
    rows_per_table = int(size_per_table / row_size)

    open(data_dir + "table_1.dat", "w").write(''.join(table1_row.format(i) for i in range(keys)))
    open(data_dir + "table_2.dat", "w").write(''.join(table2_row.format(randint(0,int(keys / key_matching))) for i in range(rows_per_table)))

if __name__ == "__main__":
    main()
