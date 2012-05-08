# HBase Backup #

Utilities to backup and restore HBase tables. Developed at [OCLC](http://www.oclc.org).

## License ##

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

## Dependencies ##

The code was tested with the following components.

* HBase 0.92.1
* Hadoop 0.20.2-cdh3u3

## Building ##

`mvn assembly:assembly`

## Usage ##

To copy HLog files every 10 minutes

`$ java org.oclc.firefly.hadoop.backup.LogCopier -d /path/to/archive/logs -m 10`

Run backup tool to get snapshots of HFiles

`$ hadoop jar HBase-Backup-1.0.4-SNAPSHOT-job.jar org.oclc.firefly.hadoop.backup.Backup`

When the command above finishes running, it will output the path of the directory containing the backup copy:

`12/04/16 15:51:42 INFO backup.Backup: Backup located at: hdfs://localhost:29318/user/espinozca/backup/bak-20120416.155126.421-20120416.155142.956`

Notice that backup directory name contains the start and end date of the backup. You can use that path as input into the import tool:

`$ java org.oclc.firefly.hadoop.backup.Import -i hdfs://localhost:29318/user/espinozca/backup/bak-20120416.155126.421-20120416.155142.956`

To get a list of the arguments these tools support, run them with the -h argument

## Overview ##

This backup utility is based around [HBASE-4618](https://issues.apache.org/jira/browse/HBASE-4618). We took the features we liked from the documents and implemented some generic tools that would work for us. This package comes equipped with three utilities: a backup tool to get snapshots of HFiles, an import/restore tool, and a tool to frequently copy HLogs. There is a fourth tool we use, but did not develop it ourselves so we have not included it in the our package. It's WALPlayer, and it is described in [HBASE-5604](https://issues.apache.org/jira/browse/HBASE-5604). WALPlayer is in the 0.94.0RC version of HBase so we back ported the code to work with HBase 0.92.1, the current version we are using.

* Backup copies snapshots of HFiles while being tolerant of several errors that may occur when copying files from HBase. It attempts to account for region splits and performs some basic verifications at the end of the backup.
* Import is used to restore the HFiles into a running HBase cluster.
* LogCopier is basic program that watches the Write-Ahead-Log directories and archives HBase log files. These logs can then be replayed with a tool like WALPlayer (See the comment above)

It is important to point out that these tools, like any other backup options in HBase at the moment, are relatively new. We are continually testing them and looking for any issues. We encourage people looking for backup solutions to try it out and send any feedback. To get more details about these tools, please take a look at the wiki page.

## Features ##
* Error tolerant. Failed regions are retried. Takes region splitting into account
* Backup individual tables or entire database
* Restore individual tables or an entire backup copy
* Backup to an external cluster
* Verify backed up regions.
* Backup and Import can be run as an application or used programmatically (The main classes is the best example)

## Future Enhancements ##
* Restore a table using a different table name

## Contributors ##

[Carlos Espinoza](mailto:espinozca@oclc.org)
