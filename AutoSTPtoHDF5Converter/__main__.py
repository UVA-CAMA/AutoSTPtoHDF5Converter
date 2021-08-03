"""
Author: Ayush Doshi

This is the starting point of the AutoSTPtoHDF5Converter to manages the arguments and the overall conversion cycle.
"""

import os
import pathlib

import configargparse

# Import module-specific functions
from Functions import *

# Get the command line arguments and parse them
parser = configargparse.ArgParser()
parser.add_argument('-conf', '--my-config', is_config_file=True, help='Config file path')
parser.add_argument('-i', '--input', help='Path to input files. ', type=str)
parser.add_argument('-o', '--output', help='Path to output folder. ', type=str)
parser.add_argument('-d', '--database', help='Path to patient info database. ', type=str)
parser.add_argument('-du', '--database_update', help='Path to folder for patient database .CSV updates will be added.',
                    type=str)
parser.add_argument('-s', '--system', help='Specify system that you are using. Default: Carescape. (u = Unity, '
                                           'p = Philips Classic, cs = Carescape, pix = Philips PIICiX)', type=str,
                    choices=['u', 'p', 'cs', 'pix'], default='cs')
parser.add_argument('-w', '--wave_data', help='Include wavedata. Default: False', action='store_true')
parser.add_argument('-del', '--delete_stp', help='Delete .STP files if conversion if successful. Default: False',
                    action='store_true')
parser.add_argument('-c', '--cores', help='Number of cores to use. Default: 6. ', type=int, default=6)
parser.add_argument('-t', '--timeout', help='Number of hours to run conversions before timeout. Default: 10 hours.',
                    type=int, default=10)
parser.add_argument('-r', '--retry_filesearch_time', help='Time, in seconds, to wait in between file searches. '
                                                          'Default: 10 min/600 sec.', type=int, default=10 * 60)
parser.add_argument('-n', '--single_hdf5_file', help='Do no split the .HDF5 file into daily .HDF5 files. '
                                                     'Default: False.', action='store_true')

args = parser.parse_args()

if __name__ == '__main__':

    # Confirm that input and output folders as well as database path have been provided; if not, ask user
    if not args.input:
        args.input = input('Path to the input folder where I should check for .stp files: ')
    if not args.output:
        args.output = input('Path to the output folder where I should put completed .hdf5 files: ')
    if not args.database:
        args.database = input('Path to the patient info database: ')
    if not args.database_update:
        args.database = input('Path to the folder where I should check for patient database updates: ')
    args.timeout = (args.timeout * 60 * 60)

    # Check the paths to folders and patient database file exists
    for path in [args.input, args.output, args.database_update]:
        if not os.path.exists(path):
            raise FileNotFoundError(f'The system cannot find the folder specified {path}')
    if not os.path.isfile(args.database):
        raise FileNotFoundError(f'The system cannot find the folder specified {args.database}')

    # Create a Processing folder for conversion workspace
    if not os.path.exists('Processing'):
        os.makedirs('Processing')
    if not os.path.exists(os.path.join(args.output, 'Converted')):
        os.makedirs(os.path.join(args.output, 'Converted'))

    # Create empty .txt files in the processing, input, and output folders to prevent deletion by os.renames()
    if not os.path.isfile(os.path.join('Processing', '_.txt')):
        pathlib.Path(os.path.join('Processing', '_.txt')).touch()
    if not os.path.isfile(os.path.join(args.output, 'Converted', '_.txt')):
        pathlib.Path(os.path.join(args.output, 'Converted', '_.txt')).touch()
    if not os.path.isfile(os.path.join(args.input, '_.txt')):
        pathlib.Path(os.path.join(args.input, '_.txt')).touch()
    if not os.path.isfile(os.path.join(args.output, '_.txt')):
        pathlib.Path(os.path.join(args.input, '_.txt')).touch()

    # Start the cycle of finding files, converting them, and outputting them
    while True:
        print("Starting new pass...")

        # Check for patient database update .CSV
        if args.database_update:
            update_patient_database(args)

        # Find potential .STP files to be converted into .HDF5
        files = find_files(args)

        # Associate .STP file to Patient ID and Offset for de-identification
        files_w_patient_info = merge_files_w_patient_info(args, files)

        # If all the found files do not have an associated offset, restart the cycle
        if files_w_patient_info.empty:
            continue

        # Convert the files and place them in the Output\Converted folder for de-identification
        convert_files(args, files_w_patient_info)

        # De-identify the names of the converted .HDF5 files and move to Output\Success
        unique_completed_files_list = deidentify_file_names(args, files_w_patient_info)

        # If Output\Converted is empty (e.g. all conversions failed), then unique_completed_files_list will be empty.
        # Restart the cycle
        if not unique_completed_files_list:
            continue

        # Update the completed files database
        update_completed_files_database(unique_completed_files_list)
