"""
Author: Ayush Doshi

Contains the "update_patient_database" function.
"""

import argparse
import glob
import os
import sqlite3

import pandas


def update_patient_database(args: argparse.Namespace) -> None:
    """
    Updates the existing PatientOffset table in the patient offset database with update .CSVs if present in the
    patient database update folder.

    :param args: argparse.Namespace that contains the arguments provided by the user.
    :type args: argparse.Namespace
    :return: None
    :rtype: None
    """

    print("Checking for patient database updates...")

    # Search folder with patient database updates for .CSV files
    update_csv_files = glob.glob(os.path.join(args.database_update, '*.csv'))

    # If update_csv_files list is not empty
    if update_csv_files:
        print("Patient update CSV(s) found. Reading them in...")

        # Pull the existing PatientOffset table from the patient offset SQLite database into a Pandas dataframe
        conn = sqlite3.connect(args.database)
        patient_info = pandas.read_sql_query('SELECT * from PatientOffset', conn)
        conn.close()

        # Create a copy to use for comparison later as well as store old column labels
        old_patient_info = patient_info.copy()
        pi_columns = patient_info.columns

        # Read in the found .CSV files into Pandas dataframes and concat them with the PatientOffset table
        for update_csv_file in update_csv_files:
            patient_info = pandas.concat([patient_info, pandas.read_csv(update_csv_file)], ignore_index=True)
            os.remove(update_csv_file)

        # Drops NA in the STPFile, PatientID, and Offset columns and removes duplicates besides the last value in the
        # STPFile column
        patient_info = patient_info.dropna(subset=pi_columns).drop_duplicates(subset=[pi_columns[0]], ignore_index=True,
                                                                              keep='last')
        patient_info = patient_info[pi_columns]

        # If there is a difference between the original patient offset table and the new one (i.e. if any new offsets
        # have been added)
        if not old_patient_info.equals(patient_info):
            print("Updating existing PatientOffset database...")

            # Replace existing patient offset table with the new patient offset table with added rows
            conn = sqlite3.connect(args.database)
            patient_info.to_sql('PatientOffset', conn, index=False, if_exists='replace')
            conn.close()

            # Check if Output\Skipped\NotInPatientDatabase folder exists
            if os.path.isdir(os.path.join(args.output, 'Skipped', 'NotInPatientDatabase')):
                print("Moving skipped files back to input folder...")

                # Find and move back any .STP files that were skipped due to there not being an associated offset in
                # the patient offset database
                skipped_files = glob.glob(os.path.join(args.output, 'Skipped', 'NotInPatientDatabase', '*.?tp'))
                for skipped_file in skipped_files:
                    os.rename(skipped_file, os.path.join(args.input, os.path.basename(skipped_file)))
        else:
            print("No new changes were found. Keeping original patient offset table...")
    else:
        print("No patient update CSVs were found...")
