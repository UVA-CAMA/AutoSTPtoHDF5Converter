import argparse
import concurrent.futures
import datetime
import glob
import logging
import multiprocessing
import numpy
import os
import shutil
import subprocess
import time
import zipfile

basedir = os.path.dirname(os.getcwd())
processingpath = os.path.join(basedir, 'Processing')

parser = argparse.ArgumentParser()
parser.add_argument('-nzx', '--no_zip_xml', help='Do not zip and save XML. Delete it only.', action='store_true')
parser.add_argument('-s', '--system', help='Specify system that you are using. Carescape is default.(u = Unity, '
                                           'p = Philips Classic, cs = Carescape, pix = Philips PIICiX)', type=str,
                    choices=['u', 'p', 'cs', 'pix'])
parser.add_argument('-w', '--wave_data', help='Include wavedata', action='store_true')
parser.add_argument('-l', '--limits', help='List of triples containing (Max Workers, Size limit of files, Timeouts in '
                                           'hours.)', type=str)
parser.add_argument('-o', '--offset', help='time string (MM/DD/YYYY) or seconds since 01/01/1970', type=str)
parser.add_argument('-d', '--delete_stp', help='Delete STP files if conversion if successful', action='store_true')
args = parser.parse_args()


def converter(inputfile, timeout):
    processstarttime = time.time()
    inputparentpath = os.path.dirname(inputfile)
    inputfilestructure = os.path.splitext(inputfile)[0]
    filebase = os.path.basename(inputfilestructure)
    processfilestructure = os.path.join(processingpath, filebase)

    paths = [processfilestructure + '.xml', processfilestructure + '.xml.zip', inputfilestructure + '.xml.zip',
             processfilestructure + '.hdf5', inputfilestructure + '.hdf5', processfilestructure + '.log',
             processfilestructure + '.log.zip', inputfilestructure + '.log.zip',
             os.path.join(processingpath, os.path.basename(inputfile))]
    for path in paths:
        if os.path.isfile(path):
            os.remove(path)

    logger = logging.getLogger(filebase)
    logger.addHandler(logging.FileHandler(paths[5]))
    logger.setLevel(logging.INFO)
    logger.propagate = False

    try:
        logger.info(f'File path is: {inputfile}')
        logger.info(f'XML path is: {paths[0]}')

        shutil.copy(inputfile, paths[8])

        logger.info(f'Starting to convert {filebase} from STP to XML...')
        stptoolkitparms = ['StpToolkit.exe', inputfile, '-xw', '-blnk', '-cs', '-o', paths[0]]
        if args.wave_data:
            stptoolkitparms.pop(2)
            logger.info('Set to output wavedata as well.')
        if args.system:
            stptoolkitparms[4] = '-' + args.system
        cmdoutput = subprocess.run(stptoolkitparms, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, timeout=timeout)
        logger.info(f'{cmdoutput.stdout} \n {cmdoutput.stderr} \n {cmdoutput.returncode}')

        os.remove(paths[8])

        logger.info(f'Starting to convert {filebase} from XML to HDF5...')
        formatconvertparms = ['formatconverter.exe', '-n', '-l', '-t', 'hdf5', '-p', '%d%i.%t', paths[0]]
        if args.offset:
            formatconvertparms.insert(3, '--offset')
            formatconvertparms.insert(4, f'-{args.offset}')
            formatconvertparms.insert(5, '-a')
            formatconvertparms.pop(2)
        cmdoutput = subprocess.run(formatconvertparms, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, timeout=timeout)
        logger.info(
            f'{cmdoutput.stdout} \n {cmdoutput.stderr} \n 'f'{cmdoutput.returncode}')

        if args.offset:
            old_seconds = filebase.split('-')[1]
            new_seconds = str(int(old_seconds) - int(args.offset))
            new_path = paths[3].replace(old_seconds, new_seconds)
            os.rename(paths[3], new_path)

        if not args.no_zip_xml:
            logger.info(f'Zipping up {filebase}.xml...')
            zipfile.ZipFile(paths[1], mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)\
                .write(paths[0], os.path.basename(paths[0]))

        logger.info(f'Deleting {filebase}.xml file...')
        os.remove(paths[0])

        logger.info(f'Moving {filebase}s HDF5, Zipped XML (if applicable), and Zipped Log files to output folder...')

        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.flush()
            handler.close()

        zipfile.ZipFile(paths[6], mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)\
            .write(paths[5], os.path.basename(paths[5]))
        os.remove(paths[5])
        pathstomove = glob.glob(os.path.join(processingpath, filebase + '*.*'))
        if args.offset:
            pathstomove.append(new_path)
        for startpath in pathstomove:
            shutil.move(startpath, inputparentpath)

    except Exception as er:
        logger.exception('Error occurred: ' + str(er))
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.flush()
            handler.close()
        paths.pop(5)
        for path in paths:
            if os.path.isfile(path):
                os.remove(path)
        raise

    return datetime.timedelta(seconds=time.time() - processstarttime)


def run_parallel(parallelfiles, maxworkers, timeout):
    try:
        print(f'Adding {len(parallelfiles)} to the queue: {"; ".join(parallelfiles)}\n')
        with concurrent.futures.ProcessPoolExecutor(max_workers=int(maxworkers)) as executor:
            globalstarttime = time.time()
            futures = \
                {executor.submit(converter, parallelfile, timeout): parallelfile for parallelfile in parallelfiles}

            donecount, timeoutcount, errorcount = 0, 0, 0
            for future in concurrent.futures.as_completed(futures):
                filename = os.path.splitext(os.path.basename(futures[future]))[0]
                try:
                    finishtime = future.result()
                    donecount += 1
                    print(f'{str(datetime.timedelta(seconds=time.time() - globalstarttime))}: '
                          f'{str(donecount + timeoutcount + errorcount)}/{str(len(parallelfiles))} finished '
                          f'(Successful: {str(donecount)}, Timeout: {str(timeoutcount)}, Error: {str(errorcount)});'
                          f' {filename} completed in {finishtime}')
                    if args.delete_stp:
                        os.remove(futures[future])
                except subprocess.TimeoutExpired:
                    timeoutcount += 1
                    print(f'{str(datetime.timedelta(seconds=time.time() - globalstarttime))}: '
                          f'{str(donecount + timeoutcount + errorcount)}/{str(len(parallelfiles))} finished '
                          f'(Successful: {str(donecount)}, Timeout: {str(timeoutcount)}, Error: {str(errorcount)});'
                          f' {filename} got stuck. Shutting down thread.')
                except Exception as e:
                    errorcount += 1
                    print(f'{str(datetime.timedelta(seconds=time.time() - globalstarttime))}: '
                          f'{str(donecount + timeoutcount + errorcount)}/{str(len(parallelfiles))} finished '
                          f'(Successful: {str(donecount)}, Timeout: {str(timeoutcount)}, Error: {str(errorcount)});'
                          f' {filename} got the error "{str(e)}"')
    except Exception:
        executor.shutdown()
        raise


if __name__ == '__main__':
    try:
        inputpath = input('Path to the input folder where I should check for .stp files (Enter for default): ')
        if not os.path.isdir(inputpath):
            inputpath = os.path.join(basedir, 'STP_Input')

        if not args.limits:
            limits_string = input('List of tuples that contain the (worker, size in GB, and timeout limits in hours.) ('
                                  f'Enter for no limits). Max workers available - {multiprocessing.cpu_count() - 1}: ')
            if limits_string:
                args.limits = limits_string
            else:
                args.limits = '(multiprocessing.cpu_count()-1, numpy.inf, 3)'

        limits = numpy.array(eval(f'[{args.limits}]'))
        workerlimits, sizelimits, timeouts = limits.T
        timeouts = (timeouts * 3600)

        filesets = [[]] * len(workerlimits)
        foundfiles = []
        files = glob.glob(os.path.join(inputpath, '**', '*.?tp'), recursive=True)
        for file in files:
            if not os.path.isfile(os.path.splitext(file)[0] + '.log.zip'):
                foundfiles.append(file)
        sizes = numpy.array([os.path.getsize(file) for file in foundfiles]) / 1e9
        sizes = numpy.digitize(sizes, sizelimits)
        foundfiles = numpy.array(foundfiles)
        for index in range(len(filesets)):
            filesets[index] = list(foundfiles[sizes == index])

        for idx in range(len(filesets)):
            if filesets[idx]:
                print(f'Found {len(filesets[idx])} new .STP files under {sizelimits[idx]}GB.')
                filesets[idx].sort(key=lambda f: os.stat(f).st_size)
                run_parallel(filesets[idx], workerlimits[idx], timeouts[idx])
            else:
                print(f'No new .STP files found under {sizelimits[idx]}GB.')

    except Exception:
        logging.shutdown()
        raise
