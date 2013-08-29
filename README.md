Google Compute Engine Cluster for Hadoop
========================================


Copyright
---------

Copyright 2013 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Disclaimer
----------

This sample application is not an official Google product.


Summary
-------

This sample application sets up Google Compute Engine instances as
a Hadoop cluster and executes MapReduce tasks.

The purpose of this sample is to demonstrate how to leverage Google
Compute Engine for parallel processing with MapReduce on Hadoop.

The sample is not meant to maintain a persistent Hadoop cluster.
Because this sample uses ephemeral disks as storage for the Hadoop cluster,
**all data on the Hadoop cluster instances, including data in
HDFS (Hadoop Distributed FileSystem), will be lost**
when the Hadoop cluster is torn down.  Important data, such as input and
output of MapReduce must be kept in persistent storage,
such as [Google Cloud Storage](https://developers.google.com/storage/).

The sample takes advantage of MapReduce tasks to parallelize the copy of
input and output of MapReduce between Google Cloud Storage and HDFS.


Prerequisites
-------------

This sample assumes Google Cloud Storage and Google Compute Engine services
are enabled from the
[Google APIs Console](https://code.google.com/apis/console/).
The sample requires sufficient Google Compute Engine quota to run
a Hadoop cluster.

The sample uses
[gsutil](https://developers.google.com/storage/docs/gsutil) and
[gcutil](https://developers.google.com/compute/docs/gcutil/),
command line tools for Google Cloud Storage and Google Compute Engine
respectively.
Make sure to have the latest version of these tools added to the PATH
environment variable.

### gcutil

This sample internally uses gcutil to perform file transfer and remote
command execution from the machine where the sample is executed, and
Google Compute Engine instances.  So it's important to set up gcutil
in an expected way.

##### Default project

Default project of gcutil must be set to the project where Hadoop cluster
is started.  In this way, `hadoop-shell.sh` can be used to log in
to Hadoop master, and use `hadooop` command there.
Run the following command to
[set gcutil default project](https://developers.google.com/compute/docs/gcutil/#project).

    gcutil getproject --project=<project ID> --cache_flag_values

##### SSH key

In order for the sample to execute remote commands automatically, gcutil must
be allowed to connect to Google Compute Engine instances without passphrase.

If gcutil has never been executed, please
[run it](https://developers.google.com/compute/docs/hello_world#addvm)
to configure SSH key.  Make sure to set empty passphrase for SSH key.

If SSH key is created earlier with passphrase, the sample fails to start
Hadoop cluster, asking for manual passphrase entry from within the automated
script.
If this happens, rename (or remove) the SSH key from the system.

    mv $HOME/.ssh/google_compute_engine $HOME/.ssh/google_compute_engine.bak

After the rename of the SSH key, run gcutil again in the same way to create
a new SSH key without passphrase.

### Environment

This sample application runs with Python 2.7.
It's tested on Mac OS X and Linux.

Alternatively, a Google Compute Engine instance can be used to run this sample,
which works as a controller of the Hadoop cluster.


Known Issues
------------

* On HDFS NameNode Web console (`http://<master external IP address>:50070`),
"Browse the filesystem" link does not work.  In order to browse HDFS
from Web UI, go to the live node list from the "Live Nodes" link, and
click the IP address of an arbitrary DataNode.

* The script does not work properly if the path to the sample application
directory contains whitespace.  Please download the sample to the path
that doesn't include any whitespaces.


Set up Instruction
------------------

### Prepare Hadoop package

##### Download Hadoop package

Download Hadoop 1.0.4 package (hadoop-1.0.4.tar.gz) from one of
[Apache Hadoop mirror sites](http://archive.apache.org/dist/hadoop/core/)

Put the downloaded package to the root directory of this sample
(`compute_engine_cluster_for_hadoop`), the same directory as hadoop-1.0.4.patch.
Download can be performed from your Web browser and the file can be copied
to the working directory.  Alternatively, command line tools, such as `curl`
or `wget` may be used.

    curl -O http://archive.apache.org/dist/hadoop/core/hadoop-1.0.4/hadoop-1.0.4.tar.gz

##### Customize Hadoop configuration and re-package

Apply hadoop-1.0.4.patch to customize Hadoop configuration.
From `compute_engine_cluster_for_hadoop` directory,
execute the following commands.

    tar zxf hadoop-1.0.4.tar.gz
    patch -p0 < hadoop-1.0.4.patch
    tar zcf hadoop-1.0.4.tar.gz hadoop-1.0.4

### Download Open JDK and Dependent Packages

The sample uses [Open JDK](http://openjdk.java.net/) as Java runtime
environment.  Open JDK Java Runtime Environment is distributed under
[GNU Public License version 2](http://www.gnu.org/licenses/gpl-2.0.html).
User must agree to the license to use Open JDK.

Create a directory called "deb_packages" under the root directory of the sample.

Download amd64 package of openjdk-6-jre-headless, and architecture-common
package of openjdk-6-jre-lib from the following sites.

* [http://packages.debian.org/wheezy/openjdk-6-jre-headless](http://packages.debian.org/wheezy/openjdk-6-jre-headless)
* [http://packages.debian.org/wheezy/openjdk-6-jre-lib](http://packages.debian.org/wheezy/openjdk-6-jre-lib)

Also download dependent packages.

* [http://packages.debian.org/wheezy/libnss3-1d](http://packages.debian.org/wheezy/libnss3-1d) [amd64]
* [http://packages.debian.org/wheezy/libnss3](http://packages.debian.org/wheezy/libnss3) [amd64]
* [http://packages.debian.org/wheezy/ca-certificates-java](http://packages.debian.org/wheezy/ca-certificates-java) [architecture-common]
* [http://packages.debian.org/wheezy/libnspr4](http://packages.debian.org/wheezy/libnspr4) [amd64]

Put the downloaded packages into the `deb_packages` directory.

    mkdir deb_packages
    cd deb_packages
    curl -O http://security.debian.org/debian-security/pool/updates/main/o/openjdk-6/openjdk-6-jre-headless_6b27-1.12.6-1~deb7u1_amd64.deb
    curl -O http://security.debian.org/debian-security/pool/updates/main/o/openjdk-6/openjdk-6-jre-lib_6b27-1.12.6-1~deb7u1_all.deb
    curl -O http://http.us.debian.org/debian/pool/main/n/nss/libnss3-1d_3.14.3-1_amd64.deb
    curl -O http://http.us.debian.org/debian/pool/main/n/nss/libnss3_3.14.3-1_amd64.deb
    curl -O http://http.us.debian.org/debian/pool/main/c/ca-certificates-java/ca-certificates-java_20121112+nmu2_all.deb
    curl -O http://http.us.debian.org/debian/pool/main/n/nspr/libnspr4_4.9.2-1_amd64.deb
    cd ..

### Prepare Google Cloud Storage bucket

Create a Google Cloud Storage bucket, from which Google Compute Engine instance
downloads Hadoop package and the generated files for Hadoop set up.

This can be done by either:

* Using existing bucket.
* Creating new bucket from
[Google Cloud Storage Web UI](https://storage.cloud.google.com/).
* Creating new bucket by
[gsutil command line tool](https://developers.google.com/storage/docs/gsutil).
`gsutil mb gs://<bucket name>`

Note this bucket can be different from the bucket where MapReduce input and
output are located.
Make sure to create the bucket in the same project as Google Compute Engine
for Hadoop cluster.

### Create client ID and client secret

Client ID and client secret are required by OAuth2 authorization
to identify the sample application.  It is required in order for the
application to access Google API (in this example, Google Compute Engine API)
on behalf of the user.

Client ID and client secret can be set up from
[API Access](https://code.google.com/apis/console/#access)
page of Google APIs Console.  Click "Create another client ID..." button
to create client ID.

Choose "Installed Application" as application type, and set application type
to "Other".

Replace `CLIENT_ID` and `CLIENT_SECRET` values in `GceCluster` class in
gce\_cluster.py with the actual values from Google APIs Console.

    CLIENT_ID = '12345....................com'
    CLIENT_SECRET = 'abcd..........'

### Download and set up Python libraries

The following libraries are required by the sample, and here is the example of
how to set up libraries in the sample application directory.

##### Google Client API

[Google Client API](http://code.google.com/p/google-api-python-client/)
is library to access various Google's services via API.

Download google-api-python-client-1.1.tar.gz from
[download page](http://code.google.com/p/google-api-python-client/downloads/list)
or by the following command.

    curl -O http://google-api-python-client.googlecode.com/files/google-api-python-client-1.1.tar.gz

Set up the library in `compute_engine_cluster_for_hadoop` directory.

    tar zxf google-api-python-client-1.1.tar.gz
    ln -s google-api-python-client-1.1/apiclient .
    ln -s google-api-python-client-1.1/oauth2client .
    ln -s google-api-python-client-1.1/uritemplate .

##### Httplib2

[Httplib2](https://code.google.com/p/httplib2/) is used by Google Client API
internally.
Download httplib2-0.8.tar.gz from
[download page](https://code.google.com/p/httplib2/downloads/list).
or by the following command.

    curl -O https://httplib2.googlecode.com/files/httplib2-0.8.tar.gz

Set up the library in `compute_engine_cluster_for_hadoop` directory.

    tar zxf httplib2-0.8.tar.gz
    ln -s httplib2-0.8/python2/httplib2 .

##### Python gflags

[gflags](http://code.google.com/p/python-gflags/) is used by Google Client API
internally.
Download python-gflags-2.0.tar.gz from
[download page](http://code.google.com/p/python-gflags/downloads/list).
or by the following command.

    curl -O http://python-gflags.googlecode.com/files/python-gflags-2.0.tar.gz

Set up the library in `compute_engine_cluster_for_hadoop` directory.

    tar zxf python-gflags-2.0.tar.gz
    ln -s python-gflags-2.0/gflags.py .
    ln -s python-gflags-2.0/gflags_validators.py .

##### Python mock (required only for unit tests)

[mock](https://pypi.python.org/pypi/mock) is mocking library for Python.
It will be included in Python as standard package from Python 3.3.
However, since this sample application uses Python 2.7, it needs to be set up.

Download mock-1.0.1.tar.gz from
[download page](https://pypi.python.org/pypi/mock#downloads).
or by the following command.

    curl -O https://pypi.python.org/packages/source/m/mock/mock-1.0.1.tar.gz

Set up the library in `compute_engine_cluster_for_hadoop` directory.

    tar zxf mock-1.0.1.tar.gz
    ln -s mock-1.0.1/mock.py .


Usage of this Sample Application
--------------------------------

### compute\_cluster\_for\_hadoop.py

`compute_cluster_for_hadoop.py` is the main script of the sample.
It sets up Google Compute Engine and Google Cloud Storage environment
for Hadoop cluster, starts up Hadoop cluster, initiates MapReduce jobs
and tears down the cluster.

##### Show usage

    ./compute_cluster_for_hadoop.py --help

`compute_cluster_for_hadoop.py` has 4 subcommands, `setup`, `start`,
`mapreduce` and `shutdown`.
Please refer to the following usages for available options.

    ./compute_cluster_for_hadoop.py setup --help
    ./compute_cluster_for_hadoop.py start --help
    ./compute_cluster_for_hadoop.py mapreduce --help
    ./compute_cluster_for_hadoop.py shutdown --help

##### Set up environment

'setup' subcommand sets up environment.

* Create SSH key used by communication between Hadoop instances.
* Upload Hadoop and Open JDK packages to Google Cloud Storage, so that
Google Compute Engine instances can download them to set up Hadoop.
* Set up firewall in Google Compute Engine network to allow users to access
Hadoop Web consoles.

'setup' must be performed at least once per combination of Google Compute Engine
project and Google Cloud Storage bucket.
'setup' may safely be run repeatedly for the same
Google Compute Engine project and/or Google Cloud Storage bucket.
Project ID can be found on "Overview" page of
[Google APIs Console](https://code.google.com/apis/console/),
and bucket name is the name of Google Cloud Storage bucket
without "gs://" prefix.

Execute the following command to set up the environment.

    ./compute_cluster_for_hadoop.py setup <project ID> <bucket name>

##### Start cluster

'start' subcommand starts Hadoop cluster.  By default, it starts 6 instances,
one master and 5 worker instances.  In other words, the default value
of the number of workers is 5.

    ./compute_cluster_for_hadoop.py start <project ID> <bucket name> [number of workers] [--prefix <prefix>]

If the instance is started for the first time, the script requires log in
and asks for authorization to access Google Compute Engine.
By default, it opens Web browser for this procedure.
If the script is run in remote host on terminal (on SSH, for example),
it cannot open Web browser on local machine.
In this case, `--noauth_local_webserver` option can be specified as instructed
by the message as follows.

    ./compute_cluster_for_hadoop.py --noauth_local_webserver start <project ID> <bucket name> [number of workers] [--prefix <prefix>]

It avoids the attempt to open local Web browser, and it shows URL for
authentication and authorization.
When authorization is successful on the Web browser, the page shows
code to paste on the terminal.
By pasting the correct code, authorization process is complete in the script.
The script can then access Google Compute Engine through API.

As shown on console log, HDFS and MapReduce Web consoles are available at
`http://<master external IP address>:50070` and
`http://<master external IP address>:50030` respectively.

'start' subcommand accepts custom command executed on each instance by
`--command` option.
For example, it can be used to install required software and/or to download
necessary files onto each instance.
The custom command is executed under the permission of the user who started
the instance.  If superuser privilege is required for the command execution,
use `sudo` in the command.

With older version of `gcutil`, the "start" subcommand fails with the
repeated following message.  Please update to the latest version of gcutil.
\([Download page](https://code.google.com/p/google-compute-engine-tools/downloads/list)\)

    FATAL Flags parsing error: Unknown command line flag 'zone'

If an error occurs resulting in Hadoop cluster set-up failure, delete existing
instances from Google Compute Engine console, fix the issues and restart.

##### Start MapReduce

'mapreduce' subcommand starts MapReduce task on the Hadoop cluster.
It requires project name and bucket for the temporary use.  The temporary
bucket is used to transfer mapper and reducer programs.  It may or may not
be the same as the bucket used to set up the cluster.

Input files must be located in a single directory on Google Cloud Storage,
from where they are copied to Hadoop cluster as mapper's input.
The directory on Google Cloud Storage or a single file on Google Cloud Storage
can be specified as input.

--input and --output parameters are required, and must point to directory
on Google Cloud Storage, starting with "gs://".  Alternatively, --input
can be a single file on Google Cloud Storage, still starting with "gs://".
--input, --output and temporary bucket can belong to the different buckets.

The output of MapReduce task is copied to the specified output directory on
Google Cloud Storage.  The existing files in the directory may be overwritten.
The output directory does not need to exist in advance.

The command uses Hadoop streaming MapReduce processing.
The mapper and the reducer must be programmed to read input from standard input
and write output to standard output.
If local file is specified as mapper and/or reducer,
they are copied to Hadoop cluster through Google Cloud Storage.
Alternatively, files on Google Cloud Storage may be used as mapper or reducer.

If mapper or reducer requires additional files, such as data files or libraries,
it can be achieved by `--command` option of 'start' subcommand.

If mapper or reducer is not specified, the step (mapper or reducer) copies
input to output.  Specifying 0 as `--reducer-count` will skip shuffle and
reduce phases, making the output of mapper the final output of MapReduce.

`sample` directory in this sample application includes sample mapper and
reducer, that counts the words' occurrence in the input files in shortest
to longest and in alphabetical order in the same length of the word.

Example:

    ./compute_cluster_for_hadoop.py mapreduce <project ID> <bucket name> [--prefix <prefix>]
        --input gs://<input directory on Google Cloud Storage>  \
        --output gs://<output directory on Google Cloud Storage>  \
        --mapper sample/shortest-to-longest-mapper.pl  \
        --reducer sample/shortest-to-longest-reducer.pl  \
        --mapper-count 5  \
        --reducer-count 1

##### Shut down cluster

'shutdown' subcommand deletes all instances in the Hadoop cluster.

    ./compute_cluster_for_hadoop.py shutdown <project ID> [--prefix <prefix>]

In this sample application, Google Compute Engine instances use
ephemeral disks for storage.  Therefore, **all files on Hadoop cluster,
including HDFS, are deleted** when the cluster is shut down.
Since this sample application is not meant to maintain Hadoop cluster
as persistent storage, important files must be kept in persistent storage,
such as Google Cloud Storage.

##### Prefix and zone

`start`, `mapreduce` and `shutdown` subcommands take string value as
"--prefix" parameter.
The prefix specified is prepended to instance names of the cluster.
In order to start up multiple clusters in the same project, specify different
prefix, and run `mapreduce` or `shutdown` commands with the appropreate prefix.

There are restrictions in the prefix string.

* Prefix must be 15 letters or less.
* Only lower case alphanumerical letters and hyphens can be used.
* The first letter must be lower case alphabet.

Similarly, if zone is specified by --zone parameter in `start` subcommand,
the same zone must be specified for `mapreduce` and `shutdown` subcommands,
so that the MapReduce task and shutdown are performed correctly.

### Log in to Hadoop master instance

hadoop-shell.sh provides convenient way to log on to the master instance
of Hadoop cluster.

    ./hadoop-shell.sh [prefix]

When logged on to the master instance, user is automatically switched
to Hadoop cluster's superuser ('hadoop').
`hadoop` command can be used to work on HDFS and Hadoop.

Example:

    hadoop dfs -ls /

### Unit tests

The sample has 3 Python files, `compute_cluster_for_hadoop.py`, `gce_cluster.py`
and `gce_api.py`.
They have corresponding unit tests, `compute_cluster_for_hadoop_test.py`,
`gce_cluster_test.py` and `gce_api_test.py` respectively.

Unit tests can be directly executed.

    ./compute_cluster_for_hadoop_test.py
    ./gce_cluster_test.py
    ./gce_api_test.py

Note some unit tests simulate error conditions, and those tests shows
error messages.
