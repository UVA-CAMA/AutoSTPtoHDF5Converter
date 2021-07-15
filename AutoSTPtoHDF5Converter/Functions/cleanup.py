import glob
import os


def cleanup(basename):
    leftover_files = glob.glob(os.path.join('Processing', basename + '*'))
    for leftover_file in leftover_files:
        os.remove(leftover_file)
