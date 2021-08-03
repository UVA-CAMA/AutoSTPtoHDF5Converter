"""
Author: Ayush Doshi

Contains the "find_files" function.
"""

import argparse
import datetime
import glob
import os
import sqlite3
import time

import pandas


def find_files(args: argparse.Namespace) -> pandas.DataFrame:
    """
    Finds .STP files that are ready to be converted to .HDF5 that have not already been converted before.

    :param args: argparse.Namespace that contains the arguments provided by the user.
    :type args: argparse.Namespace
    :return: pandas.DataFrame that contains [Path, Filename] for the files that are ready to be converted.
    :rtype: pandas.DataFrame
    """

    print("Searching for files...")

    # Recursively find initial .STP files in the input folder and add to Pandas DataFrame
    initial_files_list = glob.glob(os.path.join(args.input, '**', '*.?tp'), recursive=True)
    initial_files = pandas.DataFrame(initial_files_list, columns=['Path'])

    # Get the file size of the found initial .STP files
    initial_files['Initial Size'] = initial_files['Path'].apply(os.path.getsize)

    # Start file searching loop
    while True:

        # Wait the file search retry time provided by user arguments
        time.sleep(args.retry_filesearch_time)

        # Recursively find final .STP files in the input folder and add to Pandas DataFrame
        final_files_list = glob.glob(os.path.join(args.input, '**', '*.?tp'), recursive=True)
        final_files = pandas.DataFrame(final_files_list, columns=['Path'])

        # Get the file size of the found final .STP files
        final_files['Final Size'] = final_files['Path'].apply(os.path.getsize)

        # Merge the initial and final DataFrames on [Path] and get the difference between the start and final file sizes
        merged_files = initial_files.merge(final_files, on='Path')
        merged_files['Size Change'] = merged_files['Final Size'] - merged_files['Initial Size']

        # Get indices where the size change is 0, suggesting that the file transfer for the file has finished
        no_size_change_files_boolean = merged_files['Size Change'] == 0

        # Check if any of the size change booleans are True
        if no_size_change_files_boolean.any():

            # Select rows where the file change is 0 and save to a new DataFrame
            no_size_change_files = merged_files.loc[no_size_change_files_boolean]

            # Extract the filename from the path to a new column
            no_size_change_files['Filename'] = no_size_change_files['Path'].apply(os.path.basename)

            # Pull the CompletedFiles table from the CompletedFiles database into a Pandas DataFrame
            conn = sqlite3.connect(os.path.join('AutoSTPtoHDF5Converter', 'CompletedFiles.db'))
            completed_files = pandas.read_sql_query('SELECT CompletedFiles from CompletedFiles', conn)
            conn.close()

            # Merge the found files that have not changed in size to the completed files dataframe using the filename
            no_size_change_files = no_size_change_files.merge(completed_files, how='left', left_on='Filename',
                                                              right_on='CompletedFiles')

            # Get indices where the [CompletedFiles] column is not NA, suggesting that the filename was present in
            # the CompletedFiles database and has already been converted.
            already_completed_files_boolean = no_size_change_files['CompletedFiles'].notna()

            # Check if there are any files that have already been converted (i.e. at least 1 True in the completed files
            # boolean)
            if already_completed_files_boolean.any():
                print(f"Found {already_completed_files_boolean.sum()} file(s) that were already converted. "
                      f"Moving it/them to the skipped output folder...")

                # Select rows which point to .STP files that have already been converted
                already_completed_files = no_size_change_files.loc[already_completed_files_boolean]

                # Move the already converted files from the Input folder to the Output\Skipped\AlreadyDone folder
                [os.renames(path, os.path.join(args.output, 'Skipped', 'AlreadyDone', filename))
                 for path, filename
                 in zip(already_completed_files['Path'], already_completed_files['Filename'])]

            # Check if there are any files that have not already been converted (i.e. at least 1 False in the completed
            # files boolean)
            if not already_completed_files_boolean.all():
                print(f"Found {(~already_completed_files_boolean).sum()} new file(s) ready to be converted!")

                # Select rows which point to .STP files that have not already been converted and return the
                # [Path, Filename] columns
                new_files = no_size_change_files.loc[~already_completed_files_boolean]
                return new_files.loc[:, ['Path', 'Filename']]

        # Create a datetime object from epoch and add filesearch retry time
        d = datetime.datetime(1, 1, 1) + datetime.timedelta(seconds=args.retry_filesearch_time)

        print(f"No new files were found that are ready to be converted. Will try again in: "
              f"{d.day-1} DAYS; {d.hour} HOURS; {d.minute} MIN; {d.second} SEC;")

        # Set the initial files dataframe as the final files dataframe and restart the loop to get a new final files
        # DataFrame and check for any new changes
        initial_files = final_files.copy()
        initial_files.rename(columns={'Final Size': 'Initial Size'}, inplace=True)
