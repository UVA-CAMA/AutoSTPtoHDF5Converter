import os
import sqlite3

import pandas


def update_completed_files_database(unique_completed_files_list):

    completed_files = pandas.DataFrame(unique_completed_files_list, columns=['CompletedFiles'])

    print("Updating the CompletedFiles database with the latest completed files...")

    conn = sqlite3.connect(os.path.join('AutoSTPtoHDF5Converter', 'CompletedFiles.db'))
    completed_files.to_sql('CompletedFiles', conn, index=False, if_exists='append')
    conn.close()
