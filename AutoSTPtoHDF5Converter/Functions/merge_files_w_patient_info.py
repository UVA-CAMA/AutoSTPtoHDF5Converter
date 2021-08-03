"""
Author: Ayush Doshi

Contains the "merge_files_w_patient_info" function.
"""

import argparse
import os
import sqlite3

import pandas


def merge_files_w_patient_info(args: argparse.Namespace, files: pandas.DataFrame) -> pandas.DataFrame:
    """
    Merge paths of files that are ready to be converted with PatientID and Offset information.

    :param args: argparse.Namespace that contains the arguments provided by the user.
    :type args: argparse.Namespace
    :param files: pandas.DataFrame that contains [Path, Filename] for the files that are ready to be converted.
    :type files: pandas.DataFrame
    :return: pandas.DataFrame that contains [Path, Filename, PatientID, Offset] for the convertable files.
    :rtype: pandas.DataFrame
    """

    print("Pulling patient information for the found files...")

    # Pull the PatientOffset table from the patient offset SQLite database into a Pandas dataframe
    conn = sqlite3.connect(args.database)
    patient_info = pandas.read_sql_query('SELECT * from PatientOffset', conn)
    conn.close()

    # Rename the [STPFile] column to [Filename] column
    patient_info.rename(columns={'STPFile': 'Filename'}, inplace=True)

    # Merge the dataframe that contains files that are ready to be converted with the patient offset DataFrame
    files_w_patient_info = files.merge(patient_info, how='left', on='Filename')

    # Get indices where the Offset information is NA, suggesting that offset information cannot be found in the patient
    # offset database for that file
    not_in_patient_database_boolean = files_w_patient_info['Offset'].isna()

    # Check if any of the offset NA boolean is True
    if not_in_patient_database_boolean.any():
        print(f"Could not find patient information for {not_in_patient_database_boolean.sum()} file(s). "
              f"Moving it/them to the skipped output folder...")

        # Select rows where the offset is NA and save to a new DataFrame
        not_in_patient_database_files = files_w_patient_info.loc[not_in_patient_database_boolean]

        # Move the files that do not have patient offset information in the patient offset database from the Input
        # folder to the Output\Skipped\NotInPatientDatabase folder
        [os.renames(path, os.path.join(args.output, 'Skipped', 'NotInPatientDatabase', filename))
         for path, filename
         in zip(not_in_patient_database_files['Path'], not_in_patient_database_files['Filename'])]

    # Select rows where the offset is not NA and save to a new DataFrame
    files_w_patient_info = files_w_patient_info.loc[~not_in_patient_database_boolean]

    # Change the dtype of the PatientID and Offset to int64
    files_w_patient_info = files_w_patient_info.astype({'PatientID': 'int64', 'Offset': 'int64'})

    return files_w_patient_info
