import os
import time
import glob
import shutil
import logging
import subprocess
import zipfile
import itertools
import numpy
import datetime
import multiprocessing
import concurrent.futures

sleeptime = 60
os.chdir('C:\\Users\\Ayush\\Desktop\\STPtoHDF5WatchDog\\ProgramFiles')
basedir = os.path.dirname(os.getcwd())
dirToCheck = os.path.join(basedir, 'STP_Input')
outputstagingpath = os.path.join(basedir, 'Output_Staging')
processingpath = os.path.join(basedir, 'Processing')
logpath = os.path.join(basedir, 'Logs')
inputdir = input('Path to the input folder where I should check for new .stp files (Enter for deafult): ')
if os.path.isdir(inputdir):
    dirToCheck = inputdir


def converter(inputpath, outputpath):
    filepath = os.path.dirname(inputpath)
    filebase = os.path.splitext(os.path.basename(inputpath))[0]
    logfilepath = os.path.join(logpath, filebase + '.log')
    logger = logging.getLogger(filebase)
    logger.addHandler(logging.FileHandler(logfilepath))
    logger.setLevel(logging.INFO)
    logger.propagate = False

    try:
        logger.info(f'File path is: {inputpath}')

        xmlprocesspath = os.path.join(processingpath, filebase + '.xml')
        logger.info(f'XML Path is {xmlprocesspath}')

        logger.info(f'Starting to convert {filebase} from STP to XML...')
        output = subprocess.run(['StpToolkit.exe', inputpath, '-xw', '-blnk', '-o', xmlprocesspath],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(output.stdout)
        logger.info(output.stderr)
        logger.info(output.returncode)

        logger.info(f'Starting to convert {filebase} from XML to HDF5...')
        output = subprocess.run(
            ['formatconverter.exe', '-n', '-a', '-f', 'stpxml', '-t', 'hdf5', '-p', '%d%i.%t', xmlprocesspath],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        logger.info(output.stdout)
        logger.info(output.stderr)
        logger.info(output.returncode)

        logger.info(f'Zipping up {filebase}.xml...')
        zipfile.ZipFile(os.path.join(xmlprocesspath + '.zip'), mode='w', compression=zipfile.ZIP_DEFLATED,
                        allowZip64=True, compresslevel=9).write(xmlprocesspath)

        logger.info(f'Deleting {filebase}.Stp and {filebase}.xml files...')
        os.remove(xmlprocesspath)

        logger.info(f'Moving {filebase}s HDF5, Zipped XML, and Logs over to its folder in Output Staging...')
        selectedfiles = glob.glob(os.path.splitext(xmlprocesspath)[0] + '.*')
        for selectedfile in selectedfiles:
            logger.info(f'Moving {selectedfile} to {filepath}')
            shutil.move(selectedfile, filepath)
        logger.info(f'Moving {logfilepath} to {filepath}')
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.flush()
            handler.close()
        os.system(f'findstr /r /v "^$" {logfilepath} > {logfilepath + ".tmp"}')
        os.system(f'findstr /r /v "^$" {logfilepath + ".tmp"} > {logfilepath}')
        os.remove(logfilepath + '.tmp')
        zipfile.ZipFile(os.path.join(logfilepath + '.zip'), mode='w', compression=zipfile.ZIP_DEFLATED,
                        allowZip64=True, compresslevel=9).write(logfilepath)
        os.remove(logfilepath)
        shutil.move(os.path.join(logfilepath + '.zip'), filepath)

    except KeyboardInterrupt:
        raise
    except Exception as err:
        logger.exception('Error occurred: ' + str(err))
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.flush()
            handler.close()


if __name__ == '__main__':
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            processfutures = []
            processesfiles = []
            processboolean = []
            permanentstarttime = time.time()
            startTime = 0
            while True:
                endTime = time.time() - 10
                completedfiles = []
                path = os.path.join(dirToCheck, '**', '*.?tp')
                files = glob.glob(os.path.join(dirToCheck, '**', '*.?tp'), recursive=True)
                for file in files:
                    if startTime <= os.path.getmtime(file) < endTime:
                        if not os.path.isfile(os.path.splitext(file)[0] + '.log.zip'):
                            completedfiles.append(file)
                if completedfiles:
                    # completedfiles.sort(key=lambda f: os.stat(f).st_size)
                    print(f'\nAdding {len(completedfiles)} to the queue: {"; ".join(completedfiles)}\n')
                    for completedfile in completedfiles:
                        processfutures.append(executor.submit(converter, completedfile, outputstagingpath))
                        processesfiles.append(os.path.splitext(os.path.basename(completedfile))[0])
                startTime = endTime
                time.sleep(sleeptime)
                doneboolean = numpy.array(list(map(lambda f: f.done(), processfutures)))
                runningboolean = list(map(lambda f: f.running(), processfutures)).count(True)
                workingbooelan = list(~numpy.array(doneboolean))
                if doneboolean.any():
                    donelist = list(itertools.compress(processesfiles, doneboolean))
                    processfutures = list(itertools.compress(processfutures, workingbooelan))
                    processesfiles = list(itertools.compress(processesfiles, workingbooelan))
                    print(f'\nElapsed Time: {str(datetime.timedelta(seconds=time.time()-permanentstarttime))}; '
                          f'Total: {len(doneboolean)}; '
                          f'Running: {runningboolean}; '
                          f'Pending: {workingbooelan.count(True) - runningboolean}; '
                          f'Completed: {len(donelist)} - {"; ".join(donelist)}\n')
                else:
                    print(f'Elapsed Time: {str(datetime.timedelta(seconds=time.time()-permanentstarttime))}; '
                          f'Total: {len(doneboolean)}; '
                          f'Running: {runningboolean}; '
                          f'Pending: {workingbooelan.count(True) - runningboolean}')
    except Exception as e:
        logging.shutdown()
        executor.shutdown()
