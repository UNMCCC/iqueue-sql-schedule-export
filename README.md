Overview
--------

iQueue (https://iqueue.com/) uses a daily scheduling feed to improve the
experience of chemo patients. UNMCCC scheduling runs off a Mosaiq (Elekta)
servers

Here we produce the required fields for the feed, and a framework to automate
the export, giving the current specifications.

Automation process
------------------

Each business day a comma-delimited file containing UNMCCC patient appointments
for the day needs to be uploaded to IQ secure ftp servers

### For developers/informatics/Sysadmins only

This is an account of how we implemented the UNMCCC workflow. If you are
considering something similar for your group, make the appropriate adaptations.
Think the following: Hardcoded identifiers in the SQLquery, uses of the Mosaiq
(version 2.6), how your local servers work, etc.

Also, you should be considering alternate implementation– ideally, you would
move away from the 90’s excel-centric framework for data sharing, and you would
consider a **JSON** feed compliant with some pertinent standard. Or a GraphQL
service. Yes, we feel we know what is better – this work here is a first cut
driven by necessity to score some gains – it is far from ideal, but better than
be emailing csvs. With all that being disclosed, here is what we do:

This automated process combines batch (DOS) scripts that invoke the utilities
“sqlcmd” and “winscp”, along with system utilities (file copy, redirect). These
executables are on a timetable run by the Windows Task Scheduler.

The *sqlcmd* executes a daily iQueue script named *iQueuedDaily.sql*

The output files for sqlcmd conform to the iQueue requirements, and the
filenames contains dates of the day is produced.

Where are the executable files? The **sql** scripts and **bat** files are in the
application server hsc-impac2, under the **C:\\IQ Export** folder.

Output files are deposited in a local share of the application server
(\\export\\) where another batch file operates winscp.

A batch process (controlled by a bat. File) uploads data

One task in the Database server operated by Windows Task Scheduler call both
process (sqlcmd). 30 min later a task in the Mosaiq Outbound Interface Server
OIM server fires winscp, to ensure completion (a couple of minutes would have
suffice).

Daily schedules are called “iq\_daily\_mmm”, where mmm is the month
abbreviation, i.e.: Feb, Mar.

BAT files with the batch scripts are in the C:\\IQ Export folder, and called
iQueueExport.bat. The iQueueExport.bat file contains this sole instruction:

sqlcmd -U Username -P=password -S MOSAICDATABASE -i C:\\IQ\\IQExportDaily.sql
-s";" -W -h-1 -o
\\\\SERVERPATH\\app2\\MOSAIQ\_App\\EXPORT\\IQ\\Upload\\ELQ\_UNMCC%date:\~-4,4%\_%date:\~-10,2%.csv

![](media/67af42dd5a8fe9757ad4616f3187a716.png)

The bat file that executes a winscp script located on the mq outbound interface
server. It fires at 5am, and the bat script is  
in C:\\IQScripts and is named iQueueUploadAndMove.bat. The idea is to upload the
file securely and automatically, and move it out of the staging area into an
archival area.

![](media/6e38c475514b082edc1f4b07a543e42b.png)

Off here, iQueue analyze the data and refine the scheduling templates to
optimize the patient experience.
