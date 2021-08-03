(1) Ensure that these items are located in the same directory:
StpToolkit.exe, BedUtils.dll, ICSharpCode.SharpZipLib.dll, Newtonsoft.Json.dll, Newtonsoft.Json.xml


(2) Program Arguments. Some arguments require additional information, marked in brackets [].

-o [FilePath] = output file name otherwise filepath with .xml extension
-u = Unity (GE Direct Connect) file (default)
-p = Philips Classic file
-cs = Carescape (GE) file
-pix = Philips PIICiX file
-xp = exclude parameter data
-xw = exclude wave data
-xalrm = exclude alarms
-v = verbose
-utc = export time as UTC integer
-d = calibrate waveform data as floating point number
-blnk = remove patient name
-json = write out message as a json file instead of an xml.
-polldb = query the Bedmaster database to retrieve the patient name. Config file must be setup to point to the correct SQL connection string.
-net [IP Address][Port] = send segments over a socket instead of writing to a file.
-s [segment #] Start segment
-e [segment #] End segment
-byte [byte offset] Byte value to start from.
-stime [Time] = Time to start from. Similar to Start Segment. Must wrap time in quotes (e.g. "1/1/2015 8:00:00 AM")
-etime [Time] = Time to end from. Similar to End Segment. Must wrap time in quotes (e.g. "1/1/2015 8:00:00 AM")
-dir [Directory Entry] = Flag to process multiple Stp files at once. Following parameter must be the input directory. Will process all Stp files in that directory, up to a maximum 20 files at a time.
 
Sample Input Line for UnityWaveform (GE) files
./StpToolkit.exe MyUnityFile.Stp

Sample Input Line for ViridiaWaveform (Philips Classic) files
./StpToolkit.exe MyViridiaFile.Stp -p

Sample Input Line for PIICiXWaveform (Philips) files
./StpToolkit.exe MyPIICiXFile.Stp -pix

Sample Input Line with offsets
./StpToolkit.exe MyStpFile.Stp -u -s 123 -e 456 -xw

Sample Input Line for sending over a socket.
./StpToolkit.exe MyStpFile.Stp -d -net 127.0.0.1 701

Sample Input Line for multiple Stp file processing
./StpToolkit.exe -dir "C:\BedMasterEx\Data\CCU_BED13"


(3) StpToolkit now supports a census mode. The census will attempt to get the Patient name and start date 
from files without having to unpack the entire file. The census will search the directory and sub directories
for stp files, unless specified otherwise.

Usage:
StpToolkit.exe -census directory [Additional Program Arguments]

directory = root directory to search in.
-u = Unity (GE Direct Connect) file (default)
-p = Philips file
-cs = Carescape (GE) file.
-o [FilePath] = output file name. Excel format is default (.xlsx). Other recommended format is .txt.
-xsubdir = do not search sub directories.

Sample Input
StpToolkit -census C:\BedMasterEx\Data\CCU_11 -u -xsubdir -o C:\Test\test.xlsx




In order to run the StpToolkit executable on a Linux machine correctly, be sure that you perform the following:

(4) If you are using Ubuntu
Open a new terminal window
Type in: sudo apt-get install mono-devel


(5) If you are using Cent-OS or RedHat 
Open a new terminal window and enter the following: (If there is a higher version of mono-development, you should be safe to replace the version number in the following steps)


wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm

rpm -Uvh epel-release-6-8.noarch.rpm

yum install bison gettext glib2 freetype fontconfig libpng libpng-devel libX11 libX11-devel glib2-devel libgdi* libexif glibc-devel urw-fonts java unzip gcc gcc-c++ automake autoconf libtool make bzip2 wget

wget http://download.mono-project.com/sources/mono/mono-3.10.0.tar.bz2

tar -xvjf mono-3.10.0.tar.bz2

cd mono-3.10.0

./configure --prefix=/usr

make && make install


(Check To See If Installed Correctly)

mono --version

(Version Information should appear)


After the mono-devel kit is finished installing, change the terminal's directory to where you extracted the StpToolkit.exe location.
(Eg. cd Desktop/MyFolder)





(6) To run StpToolkit.exe, type in 

(Windows)
StpToolkit MyFile.Stp [Additional Program Arguments]

OR
(Linux)
mono StpToolkit.exe MyFile.Stp [Additional Program Arguments]

OR

./StpToolkit.exe MyFile.Stp [Additional Program Arguments]
