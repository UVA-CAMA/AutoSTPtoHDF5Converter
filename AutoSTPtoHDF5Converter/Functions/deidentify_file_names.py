"""
Author: Ayush Doshi

Contains the "deidentify_file_names" function.
"""

import argparse
import datetime
import glob
import os

import pandas


def deidentify_file_names(args: argparse.Namespace, files: pandas.DataFrame) -> [list, None]:
    """
    Deidentify the the filenames of the converted .HDF5 files found in Output\Converted and move them to Output\Success.

    :param args: argparse.Namespace that contains the arguments provided by the user.
    :type args: argparse.Namespace
    :param files: pandas.DataFrame that contains [Path, Filename, PatientID, Offset] for the convertable files.
    :type files: pandas.DataFrame
    :return: List of unique .STP file names that were converted this cycle or None.
    :rtype: [list, None]
    """

    # Get the basename from the filenames of the convertable .STP files, which would be the bed and data start in
    # epoch seconds.
    files['BedAndSeconds'] = files['Filename'].str.split('.', expand=True)[0]

    print("Getting the converted files...")

    # Get a list of paths for the converted identifiable .HDF5 files in the Output\Converted folder
    converted_files_list = glob.glob(os.path.join(args.output, 'Converted', '*.hdf5'))

    # If no .HDF5 files are found in the Output\Converted folder, return None as all conversions timed-out and/or failed
    if not converted_files_list:
        return None

    print("Splitting the converted paths for bed, seconds, and date information...")

    # Create a Pandas DataFrame of the list of paths of the converted identifiable .HDF5 files
    converted_files = pandas.DataFrame(converted_files_list, columns=['StartPath'])

    # Extract the Bed, Seconds, and Date from the converted identifiable .HDF5 file
    converted_files[['Basename', 'Extension']] = converted_files['StartPath'].apply(os.path.basename) \
        .str.split('.', expand=True)
    converted_files[['BedAndSeconds', 'Date']] = converted_files['Basename'].str.split('-_-', expand=True)
    converted_files[['Bed', 'Seconds']] = converted_files['BedAndSeconds'].str.split('-', expand=True)

    print("Merging completed files to patient ID and offsets...")

    # Merge the converted .HDF5 dataframe with the convertable .STP dataframe on [BedAndSeconds], which could
    # potentially be One-to-Many if single_hdf5_file is False, and change dtypes
    converted_files = converted_files.merge(files, on='BedAndSeconds')
    converted_files = converted_files.astype({'Seconds': 'int64', 'Offset': 'int64', 'PatientID': 'str'})

    print("De-identifying completed files and moving them to output folder structure...")

    # Set [Date] to the number of days from 1970/01/01 to the internally offset data start date and pad it to 5 digits
    converted_files['Date'] = (pandas.to_datetime(converted_files['Date']) - datetime.datetime(1969, 12, 31)).dt.days
    converted_files['Date'] = converted_files['Date'].apply('{:0>5}'.format)

    # Set [Seconds] to offset subtracted from the original file start time in epoch seconds
    converted_files['Seconds'] = (converted_files['Seconds'] - converted_files['Offset']).astype(str)

    # Set information tag to '_V' if wave_data is False and the .HDF5 only contains vital signs or '_VW' if wave_data
    # is True and the .HDF5 contains both vital signs and wave data
    if args.wave_data:
        converted_files['Information'] = '_VW'
    else:
        converted_files['Information'] = '_V'

    # Move each converted .HDF5 in the converted .HDF5 dataframe from [StartPath] in Output\Converted to
    # Output\Success with a 'PatientID\UVA_PatientID_<Days from Offset to file data start
    # day>_Bed-<Seconds-Offset>_<'_V' or '_VW'>.Extension as sub-folder and filename.
    converted_files.apply(
        lambda r: os.renames(r['StartPath'],
                             os.path.join(args.output, 'Success', r['PatientID'], 'UVA_' + r['PatientID'] + '_'
                                          + r['Date'] + '_' + r['Bed'] + '-' + r['Seconds'] + r['Information'] + '.' +
                                          r['Extension'])), axis=1)

    print("Getting a unique list of completed converted files...")

    # Return a unique list of .STP filenames that were converted this cycle, as merged [Filename] will only be not NA
    # if a match to a converted .HDF5(s) was/were found
    unique_completed_stp_files_list = list(converted_files['Filename'].unique())
    return unique_completed_stp_files_list
