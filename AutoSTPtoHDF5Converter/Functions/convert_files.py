"""
Author: Ayush Doshi

Contains the "convert_files" function as well as necessary subfunctions.
"""

import argparse
import concurrent.futures
import datetime
import glob
import os
import shutil
import subprocess
import time

import pandas


def convert_files(args: argparse.Namespace, files: pandas.DataFrame) -> None:
    """
    Parallelize the conversion of .STP to .HDF5 files using the PreVent Tools developed by Ryan Bobko and move to the
    Output\Converted staging folder for filename de-identification.

    :param args: argparse.Namespace that contains the arguments provided by the user.
    :type args: argparse.Namespace
    :param files: pandas.DataFrame that contains [Path, Filename, PatientID, Offset] for the convertable files.
    :type files: pandas.DataFrame
    :return: None
    :rtype: None
    """

    # Get [Path, Filename, Offset] from dataframe of ready files and convert to a list of tuples
    files = files.loc[:, ['Path', 'Filename', 'Offset']]
    file_tuples = files.to_records(index=False)

    print(f"Converting {len(files)} files: {file_tuples}")

    # Create a concurrent.futures multi-processing executor pool
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.cores) as executor:

        # Save the start time of the conversion process
        global_start_time = time.time()

        # Create a dictionary where the key is the submitted future job of the 'converter' function with the necessary
        # arguments and the value is the path to the file that will be converted by that future job.
        futures = \
            {executor.submit(converter, path, filename, offset, args): path
             for path, filename, offset in file_tuples}

        # Create global counters for finished, timed-out, and errored-out conversions to log progress
        done_count, timeout_count, error_count = 0, 0, 0

        # Get a future object as soon as it is completed
        for future in concurrent.futures.as_completed(futures):

            # Get the path of the completed future back using the future as the key in the futures dictionary and save
            # the filename and basename (filename w/o extension) of the path
            filename = os.path.basename(futures[future])
            basename = os.path.splitext(filename)[0]

            # The conversion job for the file faced no errors:
            try:

                # Get the conversion time and add to the done counter
                finish_time = future.result()
                done_count += 1

                # Print out the log to the console to keep track of progress
                print(f'{str(datetime.timedelta(seconds=time.time() - global_start_time))}: '
                      f'{str(done_count + timeout_count + error_count)}/{str(len(file_tuples))} done '
                      f'(Successful: {str(done_count)}, Timeout: {str(timeout_count)}, Error: {str(error_count)});'
                      f' {basename} completed in {finish_time}')

                # Delete the .STP from the input folder if desired based on the user arguments
                if args.delete_stp:
                    os.remove(futures[future])

            # The conversion job for the file timed-out in the middle of the conversion:
            except subprocess.TimeoutExpired:

                # Add to the timed-out counter
                timeout_count += 1

                # Print out the log to the console to keep track of progress
                print(f'{str(datetime.timedelta(seconds=time.time() - global_start_time))}: '
                      f'{str(done_count + timeout_count + error_count)}/{str(len(file_tuples))} done '
                      f'(Successful: {str(done_count)}, Timeout: {str(timeout_count)}, Error: {str(error_count)});'
                      f' {basename} got stuck. Shutting down thread.')

                # Move the .STP file from the Input folder to the Output\Failed\TimedOut folder
                os.renames(futures[future], os.path.join(args.output, 'Failed', 'TimedOut', filename))

            # The conversion job for the file faced an error that was not a TimeoutExpired error:
            except Exception as e:

                # Add to the errored-out counter
                error_count += 1

                # Print out the log to the console to keep track of progress
                print(f'{str(datetime.timedelta(seconds=time.time() - global_start_time))}: '
                      f'{str(done_count + timeout_count + error_count)}/{str(len(file_tuples))} done '
                      f'(Successful: {str(done_count)}, Timeout: {str(timeout_count)}, Error: {str(error_count)});'
                      f' {basename} got the error "{str(e)}"')

                # Move the .STP file from the Input folder to the Output\Failed\ErroredOut folder
                os.renames(futures[future], os.path.join(args.output, 'Failed', 'ErroredOut', filename))

            # Clean-up the processing folder, regardless of the conversion future job outcome
            finally:
                cleanup(basename)


def converter(input_stp_path: str, filename: str, offset: int, args: argparse.Namespace) -> datetime.timedelta:
    """
    Convert a given .STP file to .HDF5 file and de-identify internal timestamps using the provided negative offset from
    the patient offset database.

    :param input_stp_path: String that is the path to the .STP file in the Input folder.
    :type input_stp_path: str
    :param filename: String that is the filename of the .STP file.
    :type filename: str
    :param offset: Integer that is the offset specific to the .STP file retrieved from the patient offset database.
    :type offset: int
    :param args: argparse.Namespace that contains the arguments provided by the user.
    :type args: argparse.Namespace
    :return: datetime.timedelta for the time that the entire conversion process took.
    :rtype: datetime.timedelta
    """

    # Save the start time of the process in epoch seconds
    process_start_time = time.time()

    # Run conversion in try-finally loop to guarantee that the conversion time is returned, regardless of errors
    try:

        # Get the basename (filename w/o extension) from the filename
        basename = os.path.splitext(filename)[0]

        # Cleanup the processing folder to prevent FileExistsError
        cleanup(basename)

        # Create the paths for the .STP file and converted .XML file in the processing folder
        processing_stp_path = os.path.join('Processing', filename)
        processing_xml_path = os.path.join('Processing', basename + '.xml')

        # Copy the .STP file from the Input folder to the Processing folder
        shutil.copy(input_stp_path, processing_stp_path)

        # Create the path to the StpToolkit.exe in the UniversalFileConverter folder
        stptoolkit_path = os.path.join('AutoSTPtoHDF5Converter', 'UniversalFileConverter', 'StpToolkit.exe')

        # Create a list of parameters for subprocess.run: path to the StpToolkit, path to the .STP file in the
        # processing folder, -o flag and associated path to save the converted .XML file output, -blnk to remove any
        # existing patient data from the .STP file, and the type of EHR system used stated by user argument
        stptoolkit_params = [stptoolkit_path, processing_stp_path, '-o', processing_xml_path, '-blnk',
                             '-' + args.system]

        # If wave_data is False, add the '-xw' parameter to the list of parameters to ignore the wave_data
        if not args.wave_data:
            stptoolkit_params.append('-xw')

        # Run Windows CommandPrompt using subprocess with associated parameters while ignoring outputs and timing out
        # after the user set timeout
        subprocess.run(stptoolkit_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=args.timeout)

        # Delete the .STP file in the Processing folder
        os.remove(processing_stp_path)

        # Create the path to the formatconverter.exe in the UniversalFileConverter folder
        formatconverter_path = os.path.join('AutoSTPtoHDF5Converter', 'UniversalFileConverter', 'formatconverter.exe')

        # Create a list of parameters for subprocess.run: path to the formatconverter, -t flag and hdf5 to set the
        # output to a .HDF5 file, -C to skip caching locally, -p and %d%i-_-%s.%t to specify the output .HDF5 naming
        # structure, --offset and the specific negative offset for the file, and the .XML file in the Processing
        # folder as the input
        formatconverter_params = [formatconverter_path, '-t', 'hdf5', '-C', '-p', '%d%i-_-%s.%t',
                                  '--offset', f'-{offset}', processing_xml_path]

        # If single_hdf5_file is True, add '-n' before the .XML path to create only 1 .HDF5 file per .XML file
        # instead of creating 1 .HDF5 per day
        if args.single_hdf5_file:
            formatconverter_params.insert(-1, '-n')

        # Run Windows CommandPrompt using subprocess with associated parameters while ignoring outputs and timing out
        # after the user set timeout
        subprocess.run(formatconverter_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                       timeout=args.timeout)

        # Delete the .XML file in the Processing folder
        os.remove(processing_xml_path)

        # Find all of the .HDF5 that were created in the Processing folder with the .STP file's specific basename
        processing_hdf5_paths = glob.glob(os.path.join('Processing', basename + '-_-*.hdf5'))

        # Create the end paths for the .HDF5 files in Output\Converted and move them from the Processing folder to
        # the Output\Converted folder
        for processing_hdf5_path in processing_hdf5_paths:
            hdf5_filename = os.path.basename(processing_hdf5_path)
            converted_hdf5_path = os.path.join(args.output, 'Converted', hdf5_filename)
            shutil.move(processing_hdf5_path, converted_hdf5_path)

    finally:
        return datetime.timedelta(seconds=time.time() - process_start_time)


def cleanup(basename: str) -> None:
    """
    Delete leftover intermediate processing files in the Processing folder with a specified basename.

    :param basename: String that is the basename (filename w/o extension) of the .STP file of interest.
    :type basename: str
    :return: None
    :rtype: None
    """

    # Find any leftover processing files in the Processing folder with the specified basename and delete them
    leftover_files = glob.glob(os.path.join('Processing', basename + '*'))
    for leftover_file in leftover_files:
        os.remove(leftover_file)
