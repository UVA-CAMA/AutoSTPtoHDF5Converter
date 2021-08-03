# AutoSTPtoHDF5Converter
## What is it?

**AutoSTPtoHDF5Converter** is a Python module that is a wrapper for the [**Universal File Converter**][ufc] tools hosted
and maintained by [**UVA's Center for Advanced Medical Analytics**][cama]. This wrapper module allows for the added 
functionality of watching an input folder for incoming files, multi-core parallelization of the conversion, 
built-in de-identification of the both internal and filename timestamps.

## Where to get it?
The source code is currently hosted on GitHub at: https://github.com/UVA-CAMA/AutoSTPtoHDF5Converter.

To use, download the [**latest release**][lr] and extract to the directory you would like to run your conversions off 
of. Check the Recommendations section below on advice, including on where to run conversions. 

### Dependencies
- [Pandas](https://pandas.pydata.org/) - Required for handling SQLite connections, merges, and path manipulation of 
  multiple files.
- [ConfigArgParse][config] - Required for handling user arguments and config files.

Please install these two dependencies prior to use. Furthermore, this wrapper was written in Python 3.8 and has been 
tested with Python 3.7+. It is recommended that Python 3.7+ be used when deploying.

## How to use it?

AutoSTPtoHDF5Converter is a python module that is called from a terminal or command prompt along with arguments, some of
which are required:
```
python AutoSTPtoHDF5Converter [-h] [-conf MY_CONFIG] [-i INPUT] [-o OUTPUT] [-d DATABASE] [-du DATABASE_UPDATE] [-s {u,p,cs,pix}] [-w] [-del] [-c CORES] [-t TIMEOUT] [-r RETRY_FILESEARCH_TIME] [-n]
```

### Config and/or Command Line Setup
The arguments, which can be provided as command line arguments or config arguments, are:

Long Option | Short Option | Valid Arguments {Default} | Description
---|---|---|---
--my-config | -conf | str | Path to config file. Please look at [ConfigArgParse][config] documentation for the range of config file syntax and formats.
--input | -i | str | Path to the folder where to recursively search for .STP files at the specified interval by retry_filesearch_time.
--output | -o | str | Path to the folder where outputs should be place.
--database | -d | str | Path to the SQLite database that contains Patient Offset information. Information on necessary structure below.
--database_update | -du | str | Path to the folder where patient database .CSV updates will be placed.
--system | -s | {cs}, u, p, pix | Specify the EHR system that is used to create the data (cs = Carescape, u = Unity, p = Philips Classic, pix = Philips PIICiX).
--cores | -c | Z<sup>+</sup> int {6} | Maximum number of cores to use.
--timeout | -t | Z<sup>+</sup> int {10} | Number of hours to run conversions before timeout.
--retry_filesearch_time | -r | Z<sup>+</sup> int {600} |Time, in seconds, to wait in between file searches.
--wave_data | -w | | Include wave data in the .HDF5 file.
--delete_stp | -del | | Delete .STP file from Input folder after conversion if successful.
--single_hdf5_file | -n | | Do no split the .HDF5 file into daily .HDF5 files.

### Folder and File Setup
In addition to command line arguments or config files, certain files and folders must be setup in a specific way prior 
to use. These are:

- **Input, Output, and Patient Database Update Folder**: These three folders are required for the program to run and 
  will be asked for by the console if not initially provided. There are no restrictions on naming the folders. The only 
  restriction is that the three folders must be on the same disk or drive, which can either be local or network.
- **Patient Offset SQLite Database**: A patient offset SQLite database file is required for the program to run and will 
  be asked for by the console if not initially provided. The patient offset database file must contain a table called 
  "PatientOffset" that has 3 columns: 'STPFile' text column, 'PatientID' integer column, 'Offset' integer column. An 
  empty database with this structure, [PatientOffset.db](PatientOffset.db), has been provided as an example.
- **Patient Offset Update CSV**: A single or multiple .CSV file(s) that contains new patient offset associations to be 
  appended to the existing patient offset database without interrupting the existing cycle. Unlike previous folders and 
  files, this file is not required for the program to function. The .CSV must contain the columns 'STPFile', 'PatientID', 
  and 'Offset' so that the data is correctly appended. To append new offset associations to an existing patient offset 
  database using an update .CSV, place the .CSV with the correct headings into the patient database update folder and it
  will be included in the next conversion cycle. The addition of new patient offset associations also results in old 
  .STP files that were moved to Output\Skipped\NotInPatientDatabase before to be moved back into the input folder to 
  see if they are finally in the patient database. Duplicate STPFile associations are resolved by using the latest 
  association only. A sample patient offset update .CSV, [PatientOffsetUpdate.csv](PatientOffsetUpdate.csv), has been 
  provided as an example.

## How does it work?

**AutoSTPtoHDF5Converter** convert files by working through a repeating cycle that contains 6 overarching steps:

1. It checks the patient offset database update folder for any new .CSV files that contain new .STP to offset 
   associations, appending the new ones to the patient offset database.
2. It looks for .STP files in the input folder that are ready to be converted (i.e., size of the file is static), 
   skipping files that it has already converted. This step is also a cycle that continues until a file that is ready to 
   be converted but not already converted is found.
3. It merges the files found that are ready to be converted to their associated PatientID and Offset, skipping files 
   that do not have an associated PatientID and Offset in the patient offset database.
4. It converts .STP to .HDF5 in parallel using the Universal File Converter as well as de-identifies internal timestamps.
5. It de-identifies the filename, reorganizes the structure of the filename and parent folder, and moves it to the Output\Success folder.
6. It updates its internal list of completed .STP file conversions to prevent re-running of analyses.

## Recommendations/Advice
1. **Scratch drive**: Run the program from a drive that has a lot of free space available. These conversions often 
   require a large "Scratch" space where conversions can be done and intermediate files can be created as necessary. As 
   a result, however, a main limiting factor when it comes to conversions, especially when choosing to include wave data
   in the .HDF5 file, tends to be space as it can easily lead to errors being thrown if you do not have enough or if the
   space quickly depletes. 1 TB hard drive as the "Scratch" drive has proven useful to get around the bottlenecks due to
   space restraints as it allows for more concurrent conversions to run.
2. **Wave data**: The inclusion or exclusion of wave data greatly impacts many aspects of running 
   AutoSTPtoHDF5Converter, specifically the space needed for the conversions as well as the maximum number of cores. 
   Both of these effects arise due to the size of the .XML file and how it changes when wave data is kept or not. The 
   rule of thumb is that when wave data is not kept, the size of the .XML file is roughly 0.1x of the original 
   .HDF5 file. However, when the wave data is kept, the size of the .XML file can blow up to 20x the size of the 
   original .STP file. As a result of this, it is often necessary to use significantly less amount of cores for 
   conversions that include wave data compared to conversions that do not include wave data to preserve space. 

## License
[MIT](LICENSE)

[ufc]: https://github.com/UVA-CAMA/UniversalFileConverter
[cama]: https://github.com/UVA-CAMA
[lr]: https://github.com/UVA-CAMA/AutoSTPtoHDF5Converter/releases
[config]: https://pypi.org/project/ConfigArgParse/