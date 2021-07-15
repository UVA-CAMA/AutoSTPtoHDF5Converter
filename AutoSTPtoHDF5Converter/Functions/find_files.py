import glob
import os
import time

import pandas
import sqlite3


def find_files(args):

    print("Searching for files...")

    initial_files_list = glob.glob(os.path.join(args.input, '**', '*.?tp'), recursive=True)
    initial_files = pandas.DataFrame(initial_files_list, columns=['Path'])
    initial_files['Initial Size'] = initial_files['Path'].apply(os.path.getsize)

    while True:

        time.sleep(2)

        final_files_list = glob.glob(os.path.join(args.input, '**', '*.?tp'), recursive=True)
        final_files = pandas.DataFrame(final_files_list, columns=['Path'])
        final_files['Final Size'] = final_files['Path'].apply(os.path.getsize)

        merged_files = initial_files.merge(final_files, on='Path')
        merged_files['Size Change'] = merged_files['Initial Size'] - merged_files['Final Size']

        no_size_change_files_boolean = merged_files['Size Change'] == 0

        if no_size_change_files_boolean.any():
            no_size_change_files = merged_files.loc[no_size_change_files_boolean]
            no_size_change_files['Filename'] = no_size_change_files['Path'].apply(os.path.basename)

            conn = sqlite3.connect('CompletedFiles.db')
            completed_files = pandas.read_sql_query('SELECT CompletedFiles from CompletedFiles', conn)
            conn.close()

            no_size_change_files = no_size_change_files.merge(completed_files, how='left', left_on='Filename',
                                                              right_on='CompletedFiles')

            already_completed_files_boolean = no_size_change_files['CompletedFiles'].notna()

            if already_completed_files_boolean.any():
                print(f"Found {already_completed_files_boolean.sum()} file(s) that were already converted. "
                      f"Moving it/them to the skipped output folder...")
                already_completed_files = no_size_change_files.loc[already_completed_files_boolean]
                [os.renames(path, os.path.join(args.output, 'Skipped', 'AlreadyDone', filename))
                 for path, filename
                 in zip(already_completed_files['Path'], already_completed_files['Filename'])]

            if not already_completed_files_boolean.all():
                print(f"Found {(~already_completed_files_boolean).sum()} new file(s) ready to be converted!")
                new_files = no_size_change_files.loc[~already_completed_files_boolean]
                return new_files.loc[:, ['Path', 'Filename']]

        print("No new files were found that are ready to be converted. Will try again in a bit.")
        initial_files = final_files.copy()
        initial_files.rename(columns={'Final Size': 'Initial Size'}, inplace=True)
