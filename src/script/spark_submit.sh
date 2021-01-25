#!/bin/bash
################################################################################
#
# File: spark-submit.sh
#
# Description: Shell wrapper to execute the Load into Hbase job or extract from Hbase job.
#
# Script Modification Record
#
# Date            Name         Description
# --------------------------------------------------------------------
# JAN-21-2021      YB           Initial Creation
################################################################################

CLASS=$1
JAR=$2
CONF_FILE=$3
LOG=$4

spark-submit --master local  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  --class ${CLASS} ${JAR} ${CONF_FILE} 1>${LOG} 2>&1

RC=$?
if [[ ${RC} == 0 ]]
then
        echo "Job completed successfully!!"
else
        echo "Job FAILED!! check <${LOG}>"
fi

exit 0
