#!/bin/ksh
#--------------------------------------------------------------------------------
#
# Procedure:    etl_python_oracle_to_aerospike_and_GCP.ksh
#
# Description:  move oracle table data to flat file and then aerospike set
#
# Parameters:   none
#
#--------------------------------------------------------------------------------
# Vers|  Date  | Who | DA | Description
#-----+--------+-----+----+-----------------------------------------------------
# 1.0 |14/11/19|  MT |    | Initial Version
#--------------------------------------------------------------------------------
# 2.0 |23/02/15|  SR |    | Added a few comments
#--------------------------------------------------------------------------------
#
function F_USAGE
{
   echo "USAGE: ${1##*/} -O '<Option type>'"
   echo "USAGE: ${1##*/} -H '<HELP>' -h '<HELP>'"
   exit 10
}
#
# Main Section
#
if [[ "${1}" = "-h" || "${1}" = "-H" ]]; then
   F_USAGE $0
fi
## MAP INPUT TO VARIABLES
while getopts O: opt
do
   case $opt in
   (O) OPTION="$OPTARG" ;;
   (*) F_USAGE $0 ;;
   esac
done

[[ -z ${OPTION} ]] && print "You must specify an option 1-9 " && F_USAGE $0

ENVFILE=/home/hduser/dba/bin/environment.ksh

if [[ -f $ENVFILE ]]
then
        . $ENVFILE
else
        echo "Abort: $0 failed. No environment file ( $ENVFILE ) found"
        exit 1
fi

NOW="`date +%Y%m%d`"
#
FILE_NAME=`basename $0 .ksh`
LOG_FILE=${LOGDIR}/${FILE_NAME}.log
[ -f ${LOG_FILE} ] && rm -f ${LOG_FILE}

echo `date` " ""======= Starting $0 with option $OPTION =======" >> ${LOG_FILE}
/usr/src/Python-3.7.3/airflow_virtualenv/bin/python /home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP/src/etl_python_oracle_to_aerospike_and_GCP.py $OPTION
echo `date` " ""======= Finished $0 with option $OPTION =======" >> ${LOG_FILE}
exit

