# HBase Backup #

Utility to backup and restore HBase tables. Developed at [OCLC](http://www.oclc.org).

## License ##

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

## Dependencies ##

The code was tested with the following components.

* HBase 0.92.1
* Hadoop 0.20.2-cdh3u3

## Building ##

`mvn assembly:assembly`

## Usage ##

`hadoop jar HBase-Backup-1.0.4-SNAPSHOT-job.jar org.oclc.firefly.hadoop.backup.Backup`

When the command above finishes running, it will output the path of the directory containing the backup copy:

`12/04/16 15:51:42 INFO backup.Backup: Backup located at: hdfs://localhost:29318/user/espinozca/backup/bak-20120416.155126.421-20120416.155142.956`

Notice that backup directory name contains the start and end date of the backup. You can use that path as input into the import tool:

`hadoop jar HBase-Backup-1.0.4-SNAPSHOT-job.jar org.oclc.firefly.hadoop.backup.Import -i hdfs://localhost:29318/user/espinozca/backup/bak-20120416.155126.421-20120416.155142.956`

To get a list of the arguments these tools support, run them with the -h argument

## Overview ##

This backup utility was originally based around [HBASE-4618](https://issues.apache.org/jira/browse/HBASE-4618) but is not a full implementation of what is described in the ticket. We took the features we liked from the document and implemented a generic tool that would work for us. This package comes equipped with two utilities: a backup tool and an import/restore tool.

Backup is used for creating a copy of an HBase database and later using that copy to restore to a previous state. It does so by copying files from HBase on a per-region basis. If any failures occur while copying a file, then the entire region belonging to that file is failed, and it is postponed for the next run. There may diffferent reasons for a failure. For instance, compactions which might cause files to be merged and deleted and might also trigger region splitting. This utility takes these cases into account. When a backup completes and has no more regions to copy, it performs a form of verification, checking that the key ranges for all copied tables line up.

The backup copy is saved in a directory named with the start and end time of the backup. It is important to remember that the tool alone does not create a snapshot copy of the database, but could possibly be combined with other techniques to achieve this (Replaying logs, queueing updates to HBase while backup runs)

Lastly, the Import tool can be used to restore a previous copy to a running HBase instance. It simply takes the path to the directory created by the backup tool, moves the copied tables to HBase and registers their regions with .META.

## Features ##
* Error tolerant. Failed regions are retried. Takes region splitting into account
* Backup individual tables or entire database
* Restore individual tables or an entire backup copy
* Backup to an external cluster
* Verify backed up regions.
* Can be run as an application or used programmatically (The main class is the best example)

## Future Enhancements ##
* Restore to an offline cluster
* Restore a table using a different table name
* Ability to replay logs

## Contributors ##

[Carlos Espinoza](mailto:espinozca@oclc.org)
