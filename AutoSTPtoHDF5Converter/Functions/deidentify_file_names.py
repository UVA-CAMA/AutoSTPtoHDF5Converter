import datetime
import glob
import os

import pandas


def deidentify_file_names(args, files_w_patient_info):

    files_w_patient_info['BedAndSeconds'] = files_w_patient_info['Filename'].str.split('.', expand=True)[0]

    print("Getting the converted files...")
    converted_files_list = glob.glob(os.path.join(args.output, 'Converted', '*.hdf5'))
    if not converted_files_list:
        return None

    print("Splitting the converted paths for bed, seconds, and date information...")
    converted_files = pandas.DataFrame(converted_files_list, columns=['StartPath'])
    converted_files[['Basename', 'Extension']] = converted_files['StartPath'].apply(os.path.basename) \
        .str.split('.', expand=True)
    converted_files[['BedAndSeconds', 'Date']] = converted_files['Basename'].str.split('-_-', expand=True)
    converted_files[['Bed', 'Seconds']] = converted_files['BedAndSeconds'].str.split('-', expand=True)

    print("Merging completed files to patient ID and offsets...")
    converted_files = converted_files.merge(files_w_patient_info, on='BedAndSeconds')
    converted_files = converted_files.astype({'Seconds': 'int64', 'Offset': 'int64', 'PatientID': 'int64'})

    print("De-identifying completed files and moving them to output folder structure...")
    converted_files['Date'] = (pandas.to_datetime(converted_files['Date']) - datetime.datetime(1969, 12, 31)).dt.days
    converted_files['Date'] = converted_files['Date'].apply('{:0>3}'.format)
    converted_files['Seconds'] = (converted_files['Seconds'] - converted_files['Offset']).astype(str)
    converted_files['PatientID'] = converted_files['PatientID'].astype(str)
    converted_files.apply(
        lambda r: os.renames(r['StartPath'], os.path.join(args.output, 'Success', 'NICU', r['PatientID'],
                                                          'UVA_' + r['PatientID'] + '_' + r['Date'] + '_' + r['Bed'] +
                                                          '-' + r['Seconds'] + '.' + r['Extension'])), axis=1)

    print("Getting a unique list of completed converted files...")
    unique_completed_stp_files_list = list(converted_files['Filename'].unique())
    return unique_completed_stp_files_list
