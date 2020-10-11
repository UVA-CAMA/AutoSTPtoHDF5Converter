import concurrent.futures
import datetime
import glob
import logging
import multiprocessing
import os
import shutil
import subprocess
import time
import zipfile

sleeptime = 60
slowcpusizecutoff = 7e9
basedir = os.path.dirname(os.getcwd())
processingpath = os.path.join(basedir, 'Processing')


def converter(inputfile):
    processstarttime = time.time()
    inputparentpath = os.path.dirname(inputfile)
    inputfilestructure = os.path.splitext(inputfile)[0]
    filebase = os.path.basename(inputfilestructure)
    processfilestructure = os.path.join(processingpath, filebase)

    paths = [processfilestructure + '.xml', processfilestructure + '.xml.zip', inputfilestructure + '.xml.zip',
             processfilestructure + '.hdf5', inputfilestructure + '.hdf5', processfilestructure + '.log',
             processfilestructure + '.log.zip', inputfilestructure + '.log.zip']
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

        logger.info(f'Starting to convert {filebase} from STP to XML...')
        cmdoutput = subprocess.run(['StpToolkit.exe', inputfile, '-xw', '-blnk', '-cs', '-o', paths[0]],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(f'{cmdoutput.stdout} \n {cmdoutput.stderr} \n {cmdoutput.returncode}')

        logger.info(f'Starting to convert {filebase} from XML to HDF5...')
        cmdoutput = subprocess.run(
            ['formatconverter.exe', '-n', '-l', '-t', 'hdf5', '-p', '%d%i.%t', paths[0]],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(
            f'{cmdoutput.stdout} \n {cmdoutput.stderr} \n 'f'{cmdoutput.returncode}')

        logger.info(f'Zipping up {filebase}.xml...')
        zipfile.ZipFile(paths[1], mode='w', compression=zipfile.ZIP_DEFLATED,
                        allowZip64=True, compresslevel=9).write(paths[0], os.path.basename(paths[0]))

        logger.info(f'Deleting {filebase}.xml file...')
        os.remove(paths[0])

        logger.info(f'Moving {filebase}s HDF5, Zipped XML, and Zipped Log files to output folder...')
        zipfile.ZipFile(paths[6], mode='w', compression=zipfile.ZIP_DEFLATED,
                        allowZip64=True, compresslevel=9).write(paths[5], os.path.basename(paths[5]))
        os.remove(paths[5])
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.flush()
            handler.close()
        for startpath in [paths[1], paths[3], paths[6]]:
            shutil.move(startpath, inputparentpath)

    except Exception as er:
        logger.exception('Error occurred: ' + str(er))
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.flush()
            handler.close()
        raise

    return datetime.timedelta(seconds=time.time() - processstarttime)


def run_parallel(parallelfiles, maxworkers):
    try:
        print(f'Adding {len(parallelfiles)} to the queue: {"; ".join(parallelfiles)}\n')
        with concurrent.futures.ProcessPoolExecutor(max_workers=maxworkers) as executor:
            globalstarttime = time.time()
            futures = \
                {executor.submit(converter, parallelfile): os.path.splitext(os.path.basename(parallelfile))[0]
                 for parallelfile in parallelfiles}

            donecount, timeoutcount, errorcount = 0, 0, 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    finishtime = future.result(timeout=10800)
                    donecount += 1
                    print(f'{str(datetime.timedelta(seconds=time.time() - globalstarttime))}: '
                          f'{str(donecount + timeoutcount + errorcount)}/{str(len(parallelfiles))} finished '
                          f'(Successful: {str(donecount)}, Timeout: {str(timeoutcount)}, Error: {str(errorcount)});'
                          f' {futures[future]} completed in {finishtime}')
                except concurrent.futures.TimeoutError:
                    timeoutcount += 1
                    print(f'{str(datetime.timedelta(seconds=time.time() - globalstarttime))}: '
                          f'{str(donecount + timeoutcount + errorcount)}/{str(len(parallelfiles))} finished '
                          f'(Successful: {str(donecount)}, Timeout: {str(timeoutcount)}, Error: {str(errorcount)});'
                          f' {futures[future]} got stuck. Shutting down thread.')
                except Exception as e:
                    errorcount += 1
                    print(f'{str(datetime.timedelta(seconds=time.time() - globalstarttime))}: '
                          f'{str(donecount + timeoutcount + errorcount)}/{str(len(parallelfiles))} finished '
                          f'(Successful: {str(donecount)}, Timeout: {str(timeoutcount)}, Error: {str(errorcount)});'
                          f' {futures[future]} got the error "{str(e)}"')
    except Exception:
        executor.shutdown()
        raise


if __name__ == '__main__':
    try:
        inputpath = input('Path to the input folder where I should check for new .stp files (Enter for deafult): ')
        if not os.path.isdir(inputpath):
            inputpath = os.path.join(basedir, 'STP_Input')

        foundfiles = [[], []]
        workerlimit = [multiprocessing.cpu_count() - 2, 10]
        files = glob.glob(os.path.join(inputpath, '**', '*.?tp'), recursive=True)
        for file in files:
            if not os.path.isfile(os.path.splitext(file)[0] + '.log.zip'):
                if os.path.getsize(file) < slowcpusizecutoff:
                    foundfiles[0].append(file)
                else:
                    foundfiles[1].append(file)
        if foundfiles:
            for index in range(len(foundfiles)):
                foundfiles[index].sort(key=lambda f: os.stat(f).st_size)
                run_parallel(foundfiles[index], workerlimit[index])
        else:
            print('No .STP files that need to be converted was found.')
    except Exception:
        logging.shutdown()
        raise
