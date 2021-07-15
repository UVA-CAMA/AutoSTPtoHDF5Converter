import os
import pathlib

import configargparse
from Functions import *


parser = configargparse.ArgParser()
parser.add_argument('-conf', '--my-config', is_config_file=True, help='Config file path')
parser.add_argument('-i', '--input', help='Path to input files. ', type=str)
parser.add_argument('-o', '--output', help='Path to output folder. ', type=str)
parser.add_argument('-d', '--database', help='Path to patient info database. ', type=str)
parser.add_argument('-s', '--system', help='Specify system that you are using. Default: Carescape. (u = Unity, '
                                           'p = Philips Classic, cs = Carescape, pix = Philips PIICiX)', type=str,
                    choices=['u', 'p', 'cs', 'pix'], default='cs')
parser.add_argument('-w', '--wave_data', help='Include wavedata. Default: True', action='store_false')
parser.add_argument('-del', '--delete_stp', help='Delete STP files if conversion if successful. Default: False',
                    action='store_true')
parser.add_argument('-c', '--cores', help='Number of cores to use. Default: 6. ', type=int, default=6)
parser.add_argument('-t', '--timeout', help='Number of hours to run conversions before timeout. Default: 10', type=int,
                    default=10)

args = parser.parse_args()

if __name__ == '__main__':

    if not args.input:
        args.input = input('Path to the input folder where I should check for .stp files: ')
    if not args.output:
        args.output = input('Path to the output folder where I should put completed .hdf5 files: ')
    if not args.database:
        args.database = input('Path to the patient info database: ')
    args.timeout = (args.timeout * 60 * 60)

    if not os.path.exists('Processing'):
        os.makedirs('Processing')
    if not os.path.exists('Converted'):
        os.makedirs('Converted')

    if not os.path.isfile(os.path.join(args.input, '_.txt')):
        pathlib.Path(os.path.join(args.input, '_.txt')).touch()
    if not os.path.isfile(os.path.join(args.output, '_.txt')):
        pathlib.Path(os.path.join(args.input, '_.txt')).touch()


    while True:
        print("Starting new pass...")

        files = find_files(args)

        files_w_patient_info = merge_files_w_patient_info(args, files)

        if files_w_patient_info.empty:
            continue

        convert_files(args, files_w_patient_info)

        unique_completed_files_list = deidentify_file_names(args, files_w_patient_info)
        if not unique_completed_files_list:
            continue

        update_completed_files_database(unique_completed_files_list)
