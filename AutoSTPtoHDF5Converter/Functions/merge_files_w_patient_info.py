import os
import pandas
import sqlite3


def merge_files_w_patient_info(args, files):
    print("Pulling patient information for the found files...")
    conn = sqlite3.connect(args.database)
    patient_info = pandas.read_sql_query('SELECT * from PatientOffset', conn)
    conn.close()

    patient_info.rename(columns={'STPFile': 'Filename'}, inplace=True)

    files_w_patient_info = files.merge(patient_info, how='left', on='Filename')

    not_in_patient_database_boolean = files_w_patient_info['Offset'].isna()

    if not_in_patient_database_boolean.any():
        print(f"Could not find patient information for {not_in_patient_database_boolean.sum()} file(s). "
              f"Moving it/them to the skipped output folder...")
        not_in_patient_database_files = files_w_patient_info.loc[not_in_patient_database_boolean]
        [os.renames(path, os.path.join(args.output, 'Skipped', 'NotInPatientDatabase', filename))
         for path, filename
         in zip(not_in_patient_database_files['Path'], not_in_patient_database_files['Filename'])]

    files_w_patient_info = files_w_patient_info.loc[~not_in_patient_database_boolean]

    return files_w_patient_info
