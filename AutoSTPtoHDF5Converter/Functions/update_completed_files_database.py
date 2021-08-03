"""
Author: Ayush Doshi

Contains the "update_completed_files_database" function.
"""

import os
import sqlite3

import pandas


def update_completed_files_database(unique_completed_files_list: list) -> None:
    """
    Append the unique list of .STP files that were converted this cycle to the CompletedFiles database.

    :param unique_completed_files_list: List of unique .STP file names that were converted this cycle.
    :type unique_completed_files_list: list
    :return: None
    :rtype: None
    """

    # Create a Pandas DataFrame from the unique list of converted .STP files
    completed_files = pandas.DataFrame(unique_completed_files_list, columns=['CompletedFiles'])

    print("Updating the CompletedFiles database with the latest completed files...")

    # Append the completed files DataFrame to the existing CompletedFiles table in the CompletedFiles database
    conn = sqlite3.connect(os.path.join('AutoSTPtoHDF5Converter', 'CompletedFiles.db'))
    completed_files.to_sql('CompletedFiles', conn, index=False, if_exists='append')
    conn.close()
