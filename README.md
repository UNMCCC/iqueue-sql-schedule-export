OVERview
--------

IQ uses a daily feed to improve the scheduling of chemo patients.

We need to provide that

### What UNMCCC DOES NOW

Automation process
------------------

Each business day a comma-delimited file containing UNMCCC patient appointments
for the day needs to be uploaded to IQ secure ftp servers

### For developers/informatics/Sysadmins only

This automated process combines batch (DOS) scripts that invoke the utilities
“sqlcmd” and “winscp”, along with system utilities (file copy, redirect). These
executables are on a timetable run by the Windows Task Scheduler.

The *sqlcmd* executes a daily IQ script named *iQueuedDaily.sql*

The output files for sqlcmd conform to the IQ requirements, and the filenames
contains dates of the day is produced.

Where are the executable files? The **sql** scripts and **bat** files are in the
application server hsc-impac2, under the **C:\\IQ Export** folder.

Output files are deposited in a local share of the application server hsc-impac2
(Q:\\HDX) where another batch file operates winscp.

A batch process (controlled by a bat. File) uploads data

One task in the hsc-impac2 server operated by Windows Task Scheduler call both
process (sqlcmd). 30 min later a task in the Mosaiq Outbound Interface Server
OIM server fires winscp, to ensure completion (a couple of minutes would have
suffice).

Daily schedules are called “iq\_daily\_mmm”, where mmm is the month
abbreviation, i.e.: Feb, Mar…

BAT files with the batch scripts are in the C:\\IQ Export folder, and called
IQExport.bat. The IQExport.bat file contains this sole instruction:

sqlcmd -U Username -P=password -S MOSAICDATABASE -i C:\\IQ\\IQExportDaily.sql
-s";" -W -h-1 -o
\\\\SERVERPATH\\app2\\MOSAIQ\_App\\EXPORT\\IQ\\Upload\\ELQ\_UNMCC%date:\~-4,4%\_%date:\~-10,2%\_%date:\~-7,2%.txt

![](media/67af42dd5a8fe9757ad4616f3187a716.png)

The HDXUpload bat file that executes the winscp (upload to HDX servers in
Tennessee) is on the HSC-CC-OIM server (mq outbound interface server). It fires
at 1.30am, and the bat script is in C:\\HDXScripts and is named
HDXUploadAndMove.bat

![](media/6e38c475514b082edc1f4b07a543e42b.png)
