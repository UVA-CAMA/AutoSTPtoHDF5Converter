import concurrent.futures
import datetime
import glob
import os
import shutil
import subprocess
import time

from .cleanup import cleanup


def convert_files(args, files):
    files = files.loc[:, ['Path', 'Filename', 'Offset']]
    file_tuples = files.to_records(index=False)

    print(f"Converting {len(files)} files: {file_tuples}")

    # for path, filename, offset in file_tuples:
    #     converter(path, filename, offset, args)

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.cores) as executor:
        global_start_time = time.time()
        futures = \
            {executor.submit(converter, path, filename, offset, args): path
             for path, filename, offset in file_tuples}

        done_count, timeout_count, error_count = 0, 0, 0
        for future in concurrent.futures.as_completed(futures):
            basename = os.path.splitext(os.path.basename(futures[future]))[0]
            try:
                finish_time = future.result()
                done_count += 1
                print(f'{str(datetime.timedelta(seconds=time.time() - global_start_time))}: '
                      f'{str(done_count + timeout_count + error_count)}/{str(len(file_tuples))} done '
                      f'(Successful: {str(done_count)}, Timeout: {str(timeout_count)}, Error: {str(error_count)});'
                      f' {basename} completed in {finish_time}')
                if args.delete_stp:
                    os.remove(futures[future])
            except subprocess.TimeoutExpired:
                timeout_count += 1
                print(f'{str(datetime.timedelta(seconds=time.time() - global_start_time))}: '
                      f'{str(done_count + timeout_count + error_count)}/{str(len(file_tuples))} done '
                      f'(Successful: {str(done_count)}, Timeout: {str(timeout_count)}, Error: {str(error_count)});'
                      f' {basename} got stuck. Shutting down thread.')
                os.renames(futures[future], os.path.join(args.output, 'Failed', 'TimedOut', basename))
            except Exception as e:
                error_count += 1
                print(f'{str(datetime.timedelta(seconds=time.time() - global_start_time))}: '
                      f'{str(done_count + timeout_count + error_count)}/{str(len(file_tuples))} done '
                      f'(Successful: {str(done_count)}, Timeout: {str(timeout_count)}, Error: {str(error_count)});'
                      f' {basename} got the error "{str(e)}"')
                os.renames(futures[future], os.path.join(args.output, 'Failed', 'ErroredOut', basename))
            finally:
                cleanup(basename)


def converter(input_stp_path, filename, offset, args):
    process_start_time = time.time()

    basename = os.path.splitext(filename)[0]

    cleanup(basename)

    processing_stp_path = os.path.join('Processing', filename)
    processing_xml_path = os.path.join('Processing', basename + '.xml')

    shutil.copy(input_stp_path, processing_stp_path)

    stptoolkit_path = os.path.join('UniversalFileConverter', 'StpToolkit.exe')
    stptoolkit_params = [stptoolkit_path, processing_stp_path, '-o', processing_xml_path, '-blnk', '-' + args.system]
    if not args.wave_data:
        stptoolkit_params.append('-xw')

    subprocess.run(stptoolkit_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=args.timeout)

    os.remove(processing_stp_path)

    formatconverter_path = os.path.join('UniversalFileConverter', 'formatconverter.exe')
    formatconverter_params = [formatconverter_path, '-t', 'hdf5', '-C', '-p', '%d%i-_-%s.%t',
                              '--offset', f'-{offset}', processing_xml_path]

    subprocess.run(formatconverter_params, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=args.timeout)

    os.remove(processing_xml_path)

    processing_hdf5_paths = glob.glob(os.path.join('Processing', basename + '-_-*.hdf5'))
    for processing_hdf5_path in processing_hdf5_paths:
        hdf5_filename = os.path.basename(processing_hdf5_path)
        converted_hdf5_path = os.path.join('Converted', hdf5_filename)
        os.renames(processing_hdf5_path, converted_hdf5_path)

    return datetime.timedelta(seconds=time.time() - process_start_time)
