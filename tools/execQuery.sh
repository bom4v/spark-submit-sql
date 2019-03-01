#!/bin/bash

# Source for the SQL query execution utility:
#  https://github.com/bom4v/spark-submit-sql
# Artefacts on Maven repositories:
#  https://repo1.maven.org/maven2/org/bom4v/ti/sql-to-csv-spark_2.11/

#
TOOL_NAME="sql-to-csv-spark"
TOOL_VERSION="0.0.1"
SCALA_MJR="2.11"
SPARK_MJR="2.3"
ORG_L1="org"
ORG_L2="bom4v"
ORG_L3="ti"
MVN_REPO="https://repo1.maven.org/maven2"
TGT_DIR="target/scala-${SCALA_MJR}"
TOOL_CLS="${ORG_L1}.${ORG_L2}.${ORG_L3}.SparkClusterQueryLauncher"
#
SQL_FILENAME="hive-sql-to-csv-01-test.sql"
SQL_FILE_DIR="requests"
SQL_FILE="${SQL_FILE_DIR}/${SQL_FILENAME}"
#
HDFS_CSV_FILE="hive-generic.csv"
HDFS_DIR="incoming"
#
KRB_KEYTAB="${HOME}/${USER}.headless.keytab"
KRB_PRINCIPAL="${USER}/${HOSTNAME}"
#
has_changed_dir="0"

#
checkSQLFile() {
    if [ ! -f "${SQL_FILE}" ]
    then
	echo
	echo "The SQL script file (${SQL_FILE}) cannot be found."
	echo
	exit -1
    else
	sql_file_rel_dir="$(dirname ${SQL_FILE})"
	SQL_FILENAME="$(basename ${SQL_FILE})"
	pushd ${sql_file_rel_dir} > /dev/null 2>&1
	SQL_FILE_DIR="${PWD}"
	popd > /dev/null 2>&1
	SQL_FILE="${SQL_FILE_DIR}/${SQL_FILENAME}"
    fi
}

# Usage
if [ "$1" == "-h" -o "$1" == "--help" ]
then
    echo
    echo "Usage: $0 <path-to-sql-script-file> <csv-output-data-file-on-hdfs>"
    echo "  - Default SQL script:      ${SQL_FILE}"
    echo "  - Default output CSV file: ${HDFS_CSV_FILE}"
    echo
    exit
fi

# Parameters
if [ ! -z "$1" ]
then
    SQL_FILE="$1"
fi
checkSQLFile
if [ ! -z "$2" ]
then
    HDFS_CSV_FILE="$2"
fi

# Check for the directory where that script is launched from
if [ ! -x "tools/execQuery.sh" ]
then
    TOOL_DIR="$(dirname $0)"
    pushd ${TOOL_DIR} > /dev/null 2>&1
    pushd .. > /dev/null 2>&1
    has_changed_dir="1"
    NEW_DIR="${PWD}"
    #echo
    #echo "That script ($0) should be launched from ${NEW_DIR}; go there to see logs for instance"
    #echo "cd ${NEW_DIR} && ./tools/execQuery.sh"
    #echo
fi

# Sanity check
checkSQLFile

# Test for Kerberos
if [ ! -x "$(command -v kinit)" ]
then
    echo
    echo "The kinit utility cannot be found on that machine. It may be needed to authenticate with the Hadoop cluster"
    echo
else
    if [ -f "${KRB_KEYTAB}" ]
    then
	kinit -kt ${KRB_KEYTAB} ${KRB_PRINCIPAL} && echo "Kerberos succesfully initialized" || echo "Kerberos failed to initialize"
    fi
fi

# Test for Hadoop
if [ ! -x "$(command -v hdfs)" ]
then
    echo
    echo "The hdfs utility cannot be found on that machine. It is needed to interact with the Hadoop cluster file-system. Ask your administrator to install it"
    exit -1
else
    echo
    # Create the output directory on HDFS if not already existing
    hdfs dfs -mkdir -p ${HDFS_DIR}
    # Remove potential previous output data files
    echo "Removing potential previous output data files from ${HDFS_DIR} directory on HDFS..."
    hdfs dfs -rm -f ${HDFS_DIR}/${HDFS_CSV_FILE}
    # Reporting
    echo "Current content of the ${HDFS_DIR} output directory on HDFS:"
    hdfs dfs -ls -h ${HDFS_DIR}
    echo
fi

# Test for wget
if [ ! -x "$(command -v wget)" ]
then
    echo
    echo "The wget utility cannot be found on that machine. It is needed to download Jar artefacts. Ask your administrator to install it"
    echo
    exit -1
fi

# Test for target directory (where Jar artefacts are stored)
if [ ! -d ${TGT_DIR} ]
then
    mkdir -p ${TGT_DIR}
fi

#
JAR_FILENAME="${TOOL_NAME}_${SCALA_MJR}-${TOOL_VERSION}-spark${SPARK_MJR}.jar"
JAR_URL="${MVN_REPO}/${ORG_L1}/${ORG_L2}/${ORG_L3}/${TOOL_NAME}_${SCALA_MJR}/${TOOL_VERSION}-spark${SPARK_MJR}/${JAR_FILENAME}"
if [ ! -f ${TGT_DIR}/${JAR_FILENAME} ]
then
    echo
    echo "Downloading ${JAR_FILENAME} from Maven repository (${MVN_REPO}) into ${TGT_DIR}..."
    wget ${JAR_URL} -O ${TGT_DIR}/${JAR_FILENAME}
    echo
fi

#
echo "Launching Spark job to extract the data from the ${SQL_FILE} SQL query file..."
spark-submit --master yarn --deploy-mode client --class ${TOOL_CLS} \
	     ${TGT_DIR}/${JAR_FILENAME} ${SQL_FILE} ${HDFS_CSV_FILE}
echo "... done"
echo "Resulting data files in ${HDFS_DIR}:"
hdfs dfs -ls -h ${HDFS_DIR}
echo

# Back to the initial directory
if [ "${has_changed_dir}" == "1" ]
then
    popd > /dev/null 2>&1
    popd > /dev/null 2>&1
fi

