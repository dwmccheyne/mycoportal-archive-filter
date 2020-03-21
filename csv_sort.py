#!/usr/bin/env python3
import csv
import getopt
import itertools
import sys
from multiprocessing import Process, Manager

"""
python3 multisort.py [-i occurences.csv] [-o output.csv] [options]

The first row of the input CSV must be a header row.

Options:
  -h | --help : Prints this dialog and exits
  -i | --input : Input file, defaults to an adjacent occurences.csv file
  -o | --output : Output file, defaults to an adjacent output.csv file
  -c | --columns : The list of fields to nclude in the output
  -u | --unique : The column to filter for unique values on
  -k | --kingdoms : The list of valid kingdoms to include
"""


def usage():
    print(__doc__)


## Global Vars, you shouldn't need to change these, see Default Values in main() below
mapped_columns = []
species_set = set()


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:c:u:k:vw", ["help", "output="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
    ################################################################################
    # Default Values, edit these if you want to avoid passing arguments to --options
    ################################################################################
    infile = "occurrences.csv"
    outfile = "output.csv"
    num_workers = 4
    columns = ["kingdom", "phylum", "class", "order", "family", "genus", "scientificName", "references"]
    kingdoms = ["kingdom", "Fungi", "Protozoa", "Chromista", "Algae", "Bacteria", ""]
    unique = "scientificName"
    verbose = False
    ################################
    # Process command line arguments, don't typically edit below here
    ################################
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o == "-w":
            num_workers = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--input"):
            infile = a
        elif o in ("-o", "--output"):
            outfile = a
        elif o in ("-c", "--columns"):
            columns = a.split(',')
        elif o in ("-u", "--unique"):
            unique = a
        elif o in ("-k", "--kingdoms"):
            kingdoms = ["kingdom", ""]
            kingdoms += a.split(',')
        else:
            assert False, "unhandled option"

    ## Get column names from infile
    csv_headers = get_headers(infile)
    ## Get mapped column numbers from infile, and the unique column if set
    uc = 99999
    for c in columns:
        column_number = csv_headers.index(c)
        mapped_columns.append(column_number)
        if unique and c == unique:
            uc = mapped_columns.index(column_number)
    ## Process the data
    results = process_data(infile, num_workers, kingdoms)
    ## If there is a unique column set, filter on it.
    print(results)
    if uc is not 99999:
        filtered_rows = []
        unique_set = set()
        print(uc)
        for row in results:
            if row[uc] not in unique_set:
                unique_set.add(row[uc])
                filtered_rows.append(row)
        report(filtered_rows, outfile)
    else:
        report(results, outfile)


## Data manipulation functions
def process_data(infile, num_workers, kingdoms):
    manager = Manager()
    results = manager.list()
    work = manager.Queue(num_workers)
    # start for workers
    pool = []
    for i in range(num_workers):
        p = Process(target=process_row, args=(work, results, kingdoms))
        p.start()
        pool.append(p)
    # produce data
    with open(infile) as f:
        iters = itertools.chain(f, (None,) * num_workers)
        for num_and_line in enumerate(iters):
            work.put(num_and_line)
    for p in pool:
        p.join()
    return results


def process_row(in_queue, out_list, kingdoms):
    def map_it(row):
        # modified_row
        m_row = []
        # Create a Modified Row (m_row) using the mapped fields from the input row
        for c in mapped_columns:
            m_row.append(row[c].strip("\""))
        return m_row

    # While there is work to do
    while True:
        item = in_queue.get()
        line_no, line = item
        # exit signal
        if line is None:
            return
        # Get the corrected row (c_row) from the input row
        c_row = map_it(line.split(','))
        # If the corrected row's first column is in the accepted kingdom list, the headers, or blank include it in set
        if c_row[0] in kingdoms:
            out_list.append(c_row)
        # If not, add it to the round 2 set
        else:
            pass


def get_headers(infile):
    with open(infile, newline='') as f:
        reader = csv.reader(f)
        csv_headers = next(reader)
        return csv_headers


def report(results, outfile):
    with open(outfile, 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        for entry in enumerate(results):
            wr.writerow(entry[1])


if __name__ == "__main__":
    main()
