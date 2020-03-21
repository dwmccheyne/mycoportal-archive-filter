# mycoportal-archive-filter
Python Script to manipulate MycoPortal occurrences.csv archives and similar large csvs

TODO: Update Readme

python3 multisort.py [-i occurences.csv] [-o output.csv] [options]

The first row of the input CSV must be a header row.

Options:
  -h | --help : Prints this dialog and exits\
  -i | --input : Input file, defaults to an adjacent occurences.csv file\
  -o | --output : Output file, defaults to an adjacent output.csv file\
  -c | --columns : The list of fields to nclude in the output\
  -u | --unique : Select a column to sort for unique values on\
  -k | --kingdoms : The list of kingdoms to include\
  TODO ..
  -p | --phylums : The list of phylum to include\
  -c | --classes : The list of class to include\
  -o | --orders : The list of order to include\
  -f | --families : The list of family to include\
  -g | --genera : The list of genus to include\
  -s | --species : The list of species to include