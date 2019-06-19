SQL to CSV for Hive
===================

[![Maven Central](https://img.shields.io/maven-central/v/org.bom4v.ti/sql-to-csv-spark_2.11.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.bom4v.ti%22%20AND%20a:%22sql-to-csv-spark_2.11%22)
[![Javadocs](http://www.javadoc.io/badge/org.apache.camel/apache-camel.svg?color=brightgreen)](https://www.javadoc.io/doc/org.bom4v.ti/sql-to-csv-spark_2.11/)

# Reference
* [How to install Python virtual environments with Pyenv and `pipenv`](http://github.com/machine-learning-helpers/induction-python/tree/master/installation/virtual-env)

# Overview
That repository aims to provide simple command-line interface (CLI)
utilities to execute SQL queries, and to generate the corresponding
CSV data files, on the Hive database of Spark-based Hadoop/big data
clusters (_e.g._,
[Cloudera CDH](https://www.cloudera.com/downloads/cdh.html),
[Hortonworks HDP](https://hortonworks.com/products/data-platforms/hdp/),
[MapR](https://mapr.com/products/)).

That utility would match in features the classical SQL database
CLI/client tools such as
[MySQL CLI (`mysql`)](https://dev.mysql.com/doc/refman/8.0/en/mysql.html),
[Postgre CLI (`psql`)](https://www.postgresql.org/docs/current/app-psql.html),
[MS SQL CLI (`bcp`)](https://docs.microsoft.com/en-us/sql/tools/bcp-utility) and
[Oracle `SQL*Plus` (`sqlplus`)](https://docs.oracle.com/cd/B10501_01/server.920/a90842/ch13.htm).

It means that this Spark utility:
* Takes as input parameter the (local) path of a SQL query script
* Takes as input parameter the path of the CSV data file resulting
  from the execution of that SQL query
* Executes the SQL qeury on the Spark cluster through a direct
  `spark-submit` call (as opposed to
  [a call to either JDBC/ODBC connector
  or through the Thrift protocol](https://spark.apache.org/docs/2.3.2/sql-programming-guide.html#distributed-sql-engine))

For some reason, there does not seem to exist such tools (as of beginning
of 2019), though the Spark ecosystem of course provides various SQL-related
tools:
* [Spark SQL](https://spark.apache.org/sql/) has a
  [CLI utility](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html#running-the-spark-sql-cli),
  namely `spark-sql`, but which does not provide a way to execute queries
  from SQL script files and to generate the corresponding CSV files
* [PySpark Beeline CLI](https://spark.apache.org/docs/2.3.2/sql-programming-guide.html#distributed-sql-engine),
  which allows to extract CSV data files from SQL queries,
  but which uses the Thrift protocol through JDBC/ODBC connectors,
  therefore imposing some constraints and limits, for instance on the
  amount of records which can be extracted from the Hive database.
* [Zeppelin SQL interpreter](https://zeppelin.apache.org/docs/latest/interpreter/spark.html)
  and
  [Hive/Superset UI](https://superset.incubator.apache.org/installation.html),
  which are not CLI tools, and which have some limitations here again
  on the number of records that can be extracted from the Hive database

From [version 3 of the Hortonworks Data Platform (HDP)](https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/index.html),
Hive needs a specific connector developed by Hortonworks, namely
[Spark LLAP](https://github.com/hortonworks-spark/spark-llap),
also known as (aka)
[Hive Warehouse Connector (HWC)](https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/integrating-hive/content/hive_hivewarehouseconnector_for_handling_apache_spark_data.html).
The JAR artefacts are available on
[Hortonworks own public Maven repository](http://repo.hortonworks.com/content/repositories/releases/com/hortonworks/hive/hive-warehouse-connector_2.11/),
with a dependency on
[Hadoop AWS](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html),
for which the pure Java artefacts are available on
[Maven central repository](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/).
For some reason, SBT seems to have difficulties to resolve
the pure Java dependencies on that Hadoop AWS component, and there is
therefore an explicit dependency statement in the
[`build.sbt` file](https://github.com/bom4v/spark-submit-sql/blob/master/build.sbt).

# Installation

## Short version
* Just add the dependency on `sql-to-csv-spark` in the SBT project
  configuration (typically, `build.sbt` in the project root directory):
```scala
libraryDependencies += "org.bom4v.ti" %% "sql-to-csv-spark" % "0.0.4-spark2.3-hive3hdp"
```

* The JAR artefacts are [available on the Maven central
  repository](https://repo1.maven.org/maven2/org/bom4v/ti/sql-to-csv-spark_2.11/)

## Details
A convenient way to get the Spark ecosystem and CLI tools (_e.g._,
`spark-submit`, `spark-shell`, `spark-sql`, `beeline`, `pyspark` and `sparkR`)
is through
[PySpark](https://spark.apache.org/docs/latest/api/python/pyspark.html).
PySpark is a Python wrapper around Spark libraries, run through
a Java Virtual Machine (JVM) handily provided by
[OpenJDK](https://openjdk.java.net/).

To guarantee a full reproducibility with the Python stack, `pyenv`
and `pipenv` are used here.
Also, `.python_version` and `Pipfile` are part of the project.
The user has therefore just to install `pyenv` and `pipenv`,
and then all the commands described in this document become easily
accessible and reproducible.

Follow the instructions on
[how-to install Python and Java for Spark](http://github.com/machine-learning-helpers/induction-python/tree/master/installation/virtual-env)
for more details. Basically:
* The `pyenv` utility is installed from
  [GitHub](https://github.com/pyenv/pyenv.git)
* Specific versions of Python (namely, at the time of writing, 2.7.15
  and 3.7.2) are installed in the user account file-system.
  Those specific Python frameworks provide the `pip` package management tool
* The Python `pipenv` package is installed thanks to `pip`
* The `.python_version`, `Pipfile` and `Pipfile.lock` files, specific
  per project folder, fully drive the versions of all the Python packages
  and of Python itself, so as to guarantee full reproducibility
  on all the platforms

## Clone the Git repository
```bash
$ mkdir -p ~/dev/infra && cd ~/dev/infra
$ git clone https://github.com/bom4v/spark-submit-sql.git
$ cd ~/dev/infra/spark-submit-sql
```

## Java
* Once an OpenJDK JVM has been installed, specify `JAVA_HOME` accordingly
  in `~/.bashrc`
  
* Maven also needs to be installed

### Debian/Ubuntu
```bash
$ sudo aptitude -y install openjdk-8-jdk maven
$ export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
```

### Fedora/CentOS/RedHat
```bash
$ sudo dnf -y install java-1.8.0-openjdk maven
$ export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"
```

### MacOS
* Visit https://jdk.java.net/8/, download and install the MacOS package
```bash
$ export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
$ brew install maven
```

## SBT
[Download and install SBT](https://www.scala-sbt.org/download.html)

## Python-related dependencies
* `pyenv`:
```bash
$ git clone https://github.com/pyenv/pyenv.git ${HOME}/.pyenv
$ cat >> ~/.bashrc << _EOF

# Pyenv
# git clone https://github.com/pyenv/pyenv.git \${HOME}/.pyenv
export PATH=\${PATH}:\${HOME}/.pyenv/bin
if command -v pyenv 1>/dev/null 2>&1
then
  eval "\$(pyenv init -)"
fi

_EOF
$ . ~/.bashrc
$ pyenv install 2.7.15 && pyenv install 3.7.2
```

* `pip` and `pipenv`:
```bash
$ cd ~/dev/infra/spark-submit-sql
$ pyenv versions
  system
* 2.7.15 (set by ~/dev/infra/spark-submit-sql/.python-version)
  3.7.2
$ python --version
Python 2.7.15
$ pip install -U pip pipenv
$ pipenv install
Creating a virtualenv for this project...
Pipfile: ~/dev/infra/spark-submit-sql/Pipfile
Using ~/.pyenv/versions/2.7.15/bin/python (2.7.15) to create virtualenv...
⠇ Creating virtual environment...Using base prefix '~/.pyenv/versions/2.7.15'
New python executable in ~/.local/share/virtualenvs/spark-submit-sql-nFz46YtK/bin/python
Installing setuptools, pip, wheel...
done.
Running virtualenv with interpreter ~/.pyenv/versions/2.7.15/bin/python
✔ Successfully created virtual environment! 
Virtualenv location: ~/.local/share/virtualenvs/spark-submit-sql-nFz46YtK
Installing dependencies from Pipfile.lock (d2363d)...
  �   ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 2/2 — 00:00:20
To activate this project's virtualenv, run pipenv shell.
Alternatively, run a command inside the virtualenv with pipenv run.
```

# Use the command-line script
* Download the application JAR artefact from a
  [Maven repository](https://repo1.maven.org/maven2/):
```bash
$ wget https://oss.sonatype.org/content/groups/public/org/bom4v/ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar
```

* Launch the JAR application by specifying the input file with the SQL query
  and the output CSV file:
```bash
$ cat > sqlQuery.sql << _EOF
select 1 as testFlag
_EOF
$ spark-submit --master yarn --deploy-mode client --class org.bom4v.ti.StandaloneQueryLauncher sql-to-csv-spark_2.11-0.0.1-spark2.3.jar sqlQuery.sql output.csv
$ spark-submit --master yarn --deploy-mode client --class org.bom4v.ti.SparkClusterQueryLauncher sql-to-csv-spark_2.11-0.0.1-spark2.3.jar sqlQuery.sql output.csv
$ hdfs dfs -get incoming/output.csv
$ hdfs dfs -rm -skipTrash incoming/output.csv
$ cat output.csv
1
```

# Use the Scala library
* Write a program like:
```bash
$ mkdir -p src/main/scala/myorg
$ cat > src/main/scala/myorg/Demonstrator.scala << _EOF
import org.bom4v.ti

_EOF
```

# Contribute to that project

### Simple test
* Compile and package the SQL-based data extractor:
```bash
$ sbt 'set isSnapshot := true' compile package publishM2 publishLocal
[info] Loading settings for project spark-submit-sql-build from plugins.sbt ...
[info] Loading project definition from ~/dev/infra/spark-submit-sql/project
[info] Updating ProjectRef(uri("file:~/dev/infra/spark-submit-sql/project/"), "spark-submit-sql-build")...
[info] Done updating.
[info] Compiling 3 Scala sources to ~/dev/infra/spark-submit-sql/target/scala-2.11/classes ...
[info] Done compiling.
[success] Total time: 10 s, completed Feb 18, 2019 7:58:17 PM
[warn] Multiple main classes detected.  Run 'show discoveredMainClasses' to see the list
[info] Packaging ~/dev/infra/spark-submit-sql/target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar ...
[info] Done packaging.
[success] Total time: 1 s, completed Feb 18, 2019 7:58:18 PM
[info] Packaging ~/dev/infra/spark-submit-sql/target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3-sources.jar ...
[info] Done packaging.
[info] Wrote ~/dev/infra/spark-submit-sql/target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.pom
[info] Main Scala API documentation to ~/dev/infra/spark-submit-sql/target/scala-2.11/api...
model contains 8 documentable templates
[info] Main Scala API documentation successful.
[info] Packaging ~/dev/infra/spark-submit-sql/target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3-javadoc.jar ...
[info] Done packaging.
[info] 	published sql-to-csv-spark_2.11 to file:~/.m2/repository/org/bom4v/ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/sql-to-csv-spark_2.11-0.0.1-spark2.3.pom
[info] 	published sql-to-csv-spark_2.11 to file:~/.m2/repository/org/bom4v/ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar
[info] 	published sql-to-csv-spark_2.11 to file:~/.m2/repository/org/bom4v/ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/sql-to-csv-spark_2.11-0.0.1-spark2.3-sources.jar
[info] 	published sql-to-csv-spark_2.11 to file:~/.m2/repository/org/bom4v/ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/sql-to-csv-spark_2.11-0.0.1-spark2.3-javadoc.jar
[success] Total time: 2 s, completed Feb 18, 2019 7:58:19 PM
[info] Wrote ~/dev/infra/spark-submit-sql/target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.pom
[info] Main Scala API documentation to ~/dev/infra/spark-submit-sql/target/scala-2.11/api...
model contains 8 documentable templates
[info] Main Scala API documentation successful.
[info] Packaging ~/dev/infra/spark-submit-sql/target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3-javadoc.jar ...
[info] Done packaging.
[info] :: delivering :: org.bom4v.ti#sql-to-csv-spark_2.11;0.0.1-spark2.3 :: 0.0.1-spark2.3 :: integration :: Mon Feb 18 19:58:21 CET 2019
[info] 	delivering ivy file to ~/dev/infra/spark-submit-sql/target/scala-2.11/ivy-0.0.1-spark2.3.xml
[info] 	published sql-to-csv-spark_2.11 to ~/.ivy2/local/org.bom4v.ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/poms/sql-to-csv-spark_2.11.pom
[info] 	published sql-to-csv-spark_2.11 to ~/.ivy2/local/org.bom4v.ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/jars/sql-to-csv-spark_2.11.jar
[info] 	published sql-to-csv-spark_2.11 to ~/.ivy2/local/org.bom4v.ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/srcs/sql-to-csv-spark_2.11-sources.jar
[info] 	published sql-to-csv-spark_2.11 to ~/.ivy2/local/org.bom4v.ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/docs/sql-to-csv-spark_2.11-javadoc.jar
[info] 	published ivy to ~/.ivy2/local/org.bom4v.ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/ivys/ivy.xml
[success] Total time: 1 s, completed Feb 18, 2019 7:58:21 PM
```

* The above command generates JAR artefacts (mainly
  `sql-to-csv-spark_2.11-0.0.1-spark2.3.jar`) locally in the project `target` directory,
  as well as in the Maven and Ivy2 user repositories (`~/.m2` and
  `~/.ivy2` respectively).

* The `set isSnapshot := true` option allows to silently override
  any previous versions of the JAR artefacts in the Maven and Ivy2 repositories
  
* Check that the artefacts have been produced
  + Locally (`package` command):
```bash
$ ls -laFh target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar 
-rw-r--r-- 1 USER GROUP 4.4K Feb 13 12:16 target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar
```

  + In the local Maven repository (`publishM2` task):
```bash
$ ls -laFh ~/.m2/repository/org/bom4v/ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar
-rw-r--r-- 1 USER GROUP 4.4K Feb 13 12:16 ~/.m2/repository/org/bom4v/ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar
```

  + In the local Ivy2 repository (`publishLocal` task):
```bash
$ ls -laFh ~/.ivy2/local/org.bom4v.ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/jars/sql-to-csv-spark_2.11.jar
-rw-r--r-- 1 USER GROUP 4.4K Feb 13 12:16 ~/.ivy2/local/org.bom4v.ti/sql-to-csv-spark_2.11/0.0.1-spark2.3/jars/sql-to-csv-spark_2.11.jar
```

* Launch the job in the SBT JVM (the
  [SQL query script(`requests/hive-sql-to-csv-01-test.sql`)](https://github.com/bom4v/spark-submit-sql/blob/master/requests/hive-sql-to-csv-01-test.sql)
  and the output CSV data file (`hive-generic.csv`) are given as parameters
  at the end of the command line):
```bash
$ sbt "runMain org.bom4v.ti.StandaloneQueryLauncher requests/hive-sql-to-csv-01-test.sql hive-generic.csv"
[info] Loading settings for project spark-submit-sql-build from plugins.sbt ...
[info] Loading project definition from ~/dev/infra/spark-submit-sql/project
[info] Loading settings for project spark-submit-sql from build.sbt ...
[info] Set current project to sql-to-csv-spark (in build file:~/dev/infra/spark-submit-sql/)
[info] Running org.bom4v.ti.StandaloneQueryLauncher 
...
Spark: 2.3.2
Scala: version 2.11.12
File-path for the SQL query: requests/hive-sql-to-csv-01-test.sql
File-path for the expected CSV file: hive-generic.csv
SQL query:  select 1 as test
[success] Total time: 8 s, completed Feb 18, 2019 8:14:27 PM
```

* One can see that the content of the
  [SQL query file (`requests/hive-sql-to-csv-01-test.sql`)](https://github.com/bom4v/spark-submit-sql/blob/master/requests/hive-sql-to-csv-01-test.sql)
  is `select 1 as test`, as expected

* Launch the job with `spark-submit` (the
  [SQL query script(`requests/hive-sql-to-csv-01-test.sql`)](https://github.com/bom4v/spark-submit-sql/blob/master/requests/hive-sql-to-csv-01-test.sql)
  and the output CSV data file (`hive-generic.csv`) are given as parameters
  at the end of the command line)
  + In local mode (for instance, on a laptop; that mode may not always work
    on the Spark/Hadoop clusters):
```bash
$ pipenv run spark-submit --master local --class org.bom4v.ti.StandaloneQueryLauncher target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar requests/hive-sql-to-csv-01-test.sql hive-generic.csv
2019-02-18 20:22:46 INFO  SparkContext:54 - Running Spark version 2.4.0
2019-02-18 20:22:46 INFO  SparkContext:54 - Submitted application: StandaloneQuerylauncher
...
Spark: 2.4.0
Scala: version 2.11.12
File-path for the SQL query: requests/hive-sql-to-csv-01-test.sql
File-path for the expected CSV file: hive-generic.csv
SQL query:  select 1 as test
...
2019-02-18 20:22:47 INFO  SparkContext:54 - Successfully stopped SparkContext
2019-02-18 20:22:47 INFO  ShutdownHookManager:54 - Shutdown hook called
...
```

  + In Yarn cluster client mode with the standalone version (that method
    is basically the same as above):
```bash
$ run spark-submit --num-executors 1 --executor-memory 512m --master yarn --deploy-mode client --class org.bom4v.ti.StandaloneQueryLauncher target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar requests/hive-sql-to-csv-01-test.sql hive-generic.csv
...
Spark: 2.4.0
Scala: version 2.11.12
File-path for the SQL query: requests/hive-sql-to-csv-01-test.sql
File-path for the expected CSV file: hive-generic.csv
SQL query:  select 1 as test
...
```

  + In Yarn cluster client mode with an actual Spark client request
    (for instance, on the Spark/Hadoop clusters;
    that method needs a proper setup of those clusters in order to work,
	including the `SPARK_HOME` or `YARN_CONF_DIR` environment variable set;
	the `spark-submit` is not prefixed by `pipenv run` here because that
	command is provided by the Spark cluster client tools, for instance
	`/usr/hdp/current/spark2-client` on Hortonworks Hadoop distributions):
```bash
$ SPARK_HOME="/usr/hdp/current/spark2-client"
$ spark-submit --num-executors 1 --executor-memory 512m --master yarn --deploy-mode client --class org.bom4v.ti.SparkClusterQueryLauncher target/scala-2.11/sql-to-csv-spark_2.11-0.0.1-spark2.3.jar requests/hive-sql-to-csv-01-test.sql hive-generic.csv
SPARK_MAJOR_VERSION is set to 2, using Spark2
19/02/14 10:44:57 INFO SparkContext: Running Spark version 2.3.2.2.6.4.0-91
19/02/14 10:44:58 INFO SparkContext: Submitted application: SparkClusterQueryLauncher
19/02/14 10:45:03 INFO Client: Submitting application application_1549030509994_0353 to ResourceManager
19/02/14 10:45:03 INFO YarnClientImpl: Submitted application application_1549030509994_0353
...
Spark: 2.3.2.2.6.4.0-91
Scala: version 2.11.8
...
19/02/14 10:45:20 INFO DAGScheduler: ResultStage 1 (show at QueryLauncher.scala:92) finished in 0.112 s
19/02/14 10:45:21 INFO DAGScheduler: Job 0 finished: show at QueryLauncher.scala:92, took 4.422912 s
+----+
|test|
+----+
|   1|
+----+
...
19/02/14 10:45:22 INFO SparkContext: Successfully stopped SparkContext
19/02/14 10:45:22 INFO ShutdownHookManager: Shutdown hook called
```

* A small Shell script,
  [namely `tools/execQuery.sh`](https://github.com/bom4v/spark-submit-sql/blob/master/tools/execQuery.sh),
  is available to ease the launch of those Spark jobs on Hadoop/Spark clusters:
```bash
$ ./tools/execQuery.sh -h
Usage: ./tools/execQuery.sh <path-to-sql-script-file> <csv-output-data-file-on-hdfs>
  - Default SQL script:      requests/hive-sql-to-csv-01-test.sql
  - Default output CSV file: hive-generic.csv
```
That tool automatically downloads the Jar artefact if needed, and checks
the `incoming` HDFS directory when the Spark job completes.

* Check the resulting (compressed) CSV file.
  On recent versions of Hadoop (_e.g._, 3.1+), the data files produced
  by Spark are compressed (deflated in Hadoop parlance). After the merge
  process, they have to be uncompressed (inflated) before they can be read
  by a human being. An easy way to uncompress files is shown below thanks
  to Perl (inspired by a
  [post on StackOverflow](https://stackoverflow.com/questions/18759142/how-to-read-a-deflate-file-in-hadoop)):
```bash
$ alias undeflate="perl -MCompress::Zlib -e 'undef $/; print uncompress(<>)'"
$ hdfs dfs -ls -h incoming/hive-generic.csv
Found 1 items
-rw-r--r--   3 USER GROUP  59 B 2019-02-14 13:27 incoming/hive-generic.csv
$ hdfs dfs -tail incoming/hive-generic.csv | undeflate
1
$ hdfs dfs -cat incoming/hive-generic.csv | undeflate | wc -l
1
```

* For bigger files (_e.g._, hundreds of GB), you may want to recompress
  the file on the fly, for instance:
```bash
$ hdfs dfs -cat incoming/hive-generic-big-file.csv | undeflate | bzip2 -c | hdfs dfs -put - incoming/hive-generic-big-file.csv.bz2
```

