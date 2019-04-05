#!/bin/bash

## This script to load the data from tmo related csv files to tmo related database tables.


## Processing directory
PRODIR="/tmp/integration/processed"
ERRDIR="/tmp/integration/error/"
LOGDIR="/tmp/integration/logs/"
TEMPDIR="/tmp/integration/temp/"
FILESDIR="/tmp/integration/inbound/"

#Declaring mysql DB connection

MASTER_DB_USER='<username>'
MASTER_DB_PASSWD='<password>'
MASTER_DB_PORT=3306
MASTER_DB_HOST='<db server name>'
MASTER_DB_NAME='<dbname>'

SQL_Query='select * from TmpCase limit 10'


function loadTempTable()
{
        echo -e "\t INFO : Loading .... file `basename $1` to temporory table"
        TABLEFILENAME_1=`basename $1 | cut -d "." -f1`
        ##TEMPTABLEFILENAME=`echo "${TABLEFILENAME_1}_temp.csv"`
        TEMPTABLEFILENAME=`basename $1`
        COLUMNNAMES=`grep -i "${TABLEFILENAME_1}_COLUMNS" tablecolumn.config | awk -F '"' '{print $2}'`

        #echo -e "Import column names are\n ${COLUMNNAMES}"


        mysqlimport --ignore-lines=1 --fields-terminated-by=, --verbose --local --delete --fields-optionally-enclosed-by='"' --host=tfb-integ
ration.cmvubf6cpu8u.us-west-2.rds.amazonaws.com --user <username> --password=<password> ${COLUMNNAMES} tfbintegration ${TEMPDIR}/${TABLEFILE
NAME_1}/${TEMPTABLEFILENAME}

         if [ $? != 0 ]
        then
                mv ${TEMPDIR}/${TABLEFILENAME_1}/*.csv ${ERRDIR}`basename $1`.$(date "+%Y.%m.%d-%H.%M.%S")

        fi

         if [ $? == 0 ]
        then
                                mv ${TEMPDIR}/${TABLEFILENAME_1}/*.csv ${PRODIR}/`basename $1`.$(date "+%Y.%m.%d-%H.%M.%S")
                 echo "Success"
        fi
		        echo -e "\tINFO : Importing completed on table with file `basename $1`."
}
## End of loadTempTable


function pushUpdateOrInsert()
{
        TABLENAME=`echo $1`

        SQL_INSERT=`echo "insert into ${TABLENAME} Select * from ${TABLENAME}_temp where not exists (Select 1 from ${TABLENAME} where ${TABLE
NAME}.id = ${TABLENAME}_temp.id)"`

        echo -e "\tINFO :Insert begins"
mysql -u$MASTER_DB_USER -p$MASTER_DB_PASSWD -P$MASTER_DB_PORT -h$MASTER_DB_HOST -D$MASTER_DB_NAME <<EOF
$SQL_INSERT;
EOF

        SQL_UPDATE_COUNT=`grep -i "${TABLENAME}_UPDATE" QueryConfig.sql | awk -F '"' '{print $2}' | wc -l`
        if [ "$SQL_UPDATE_COUNT" -gt 1 ];then
                echo -e "\tERROR : There are multiple Update sql in configuration file"
                exit;
        elif [ "$SQL_UPDATE_COUNT" -lt 1 ];then
                echo -e "\tERROR : There is no update sql present in configuation file"
                exit;
        elif [ "$SQL_UPDATE_COUNT" -eq 1 ];then
                echo -e "\tINFO : Performing update "
                SQL_UPDATE=`grep -i "${TABLENAME}_UPDATE" QueryConfig.sql | awk -F '"' '{print $2}'`
                echo -e "\tINFO : Update begins"
mysql -u$MASTER_DB_USER -p$MASTER_DB_PASSWD -P$MASTER_DB_PORT -h$MASTER_DB_HOST -D$MASTER_DB_NAME <<EOF
$SQL_UPDATE;
EOF
        fi
}
## End of pushUpdateOrInsert

function verifyfile()
{
        echo "INFO : Verification of files columns start"
        TABLEFILENAME_1=`basename $1 | cut -d "." -f1`
        TEMPTABLEFILENAME=`basename $1`
        ACTUALFILENAME=`basename $2`
        ERRORFILE="ERROR_${TABLEFILENAME_1}.txt"

echo "TABLEFILENAME_1 ${TABLEFILENAME_1}  TEMPTABLEFILENAME ${TEMPTABLEFILENAME} ACTUALFILENAME ${ACTUALFILENAME}"

        COLUMNNAMES=`grep -i "${TABLEFILENAME_1}_COLUMNS" tablecolumn.config | awk -F '"' '{print $2}'`
		 echo ${COLUMNNAMES} | awk -F "=" '{print $2}' | awk -F"," '{for(i=1;i<=NF;i++) printf($i"\n")}' > ${TABLEFILENAME_1}_querycolumnpatterns.txt

## Taking from File

        awk 'NR==1' ${TEMPDIR}/${TABLEFILENAME_1}/${TEMPTABLEFILENAME} | awk -F"," '{for(i=1;i<=NF;i++) printf($i"\n")}' > V2_${TABLEFILENAME
_1}_csvcolumpatterns.txt
//" V2_${TABLEFILENAME_1}_csvcolumpatterns.txt >${TABLEFILENAME_1}_csvcolumpatterns.txt;
        rm V2_${TABLEFILENAME_1}_csvcolumpatterns.txt;

        QUERYCOLUMNSIZE=`wc -l ${TABLEFILENAME_1}_querycolumnpatterns.txt | awk '{print $1}'`
        CSVCOLUMNSIZE=`wc -l ${TABLEFILENAME_1}_csvcolumpatterns.txt  | awk '{print $1}'`

        if [ "$QUERYCOLUMNSIZE" -ne "$CSVCOLUMNSIZE" ];then
                echo "ERROR : Input csv file column numbers are not matching with predefined template"
                mv ${TEMPDIR}/${TABLEFILENAME_1}/${TEMPTABLEFILENAME} ${ERRDIR}${ACTUALFILENAME}
                echo "ERROR : Exiting execution of program"
                exit;
        else
                echo "INFO : Number of columns are matching"
                diff ${TABLEFILENAME_1}_querycolumnpatterns.txt ${TABLEFILENAME_1}_csvcolumpatterns.txt > /dev/null;
                DIFFSTATUS=$?
                if [ "$DIFFSTATUS" -ne 0 ];then
                        echo "ERROR : Input csv column positions are not matching with predefined template"
                        mv ${TEMPDIR}/${TABLEFILENAME_1}/${TEMPTABLEFILENAME} $$ERRDIR}${ACTUALFILENAME}
                        echo "ERROR : Exiting execution of program"
                        exit;
                else
                        > ${ERRDIR}${ERRORFILE};
                        awk -v LNCOUNT=${CSVCOLUMNSIZE} -F"," 'FNR>=2 {if (NF != LNCOUNT) print "Error : Line--> "NR " have "  NF " Records";
}' ${TEMPDIR}/${TABLEFILENAME_1}/${TEMPTABLEFILENAME} >  ${ERRDIR}${ERRORFILE}

                        if [ `wc -l  ${ERRDIR}${ERRORFILE} | awk '{print $1}'` -gt 0 ]; then echo -e "\tERROR : Input csv file have errors.\n
Please refer  ${ERRDIR}${ERRORFILE} for details.. "; fi
                        mv ${TEMPDIR}/${TABLEFILENAME_1}/${TEMPTABLEFILENAME} ${ERRDIR}${ACTUALFILENAME}
                        echo "ERROR : Exiting execution of program"
                        exit;
                fi
        fi
}
## End of verifyfile

####### *** main section start *** #######
##########################################

INPUTTABLENAME=`echo $1`

echo "Execution starts on `date` "
echo "------------------------------------"

#Checking input parameter is available
if [ -z "$1" ];then
        echo -e "\nPARAMETER ERROR : There is no table name passed as parameter\n"
        exit;
else
        LINE=`echo $1`
        echo "INFO : Processing on table ${LINE} START"
        FILESDIR=${FILESDIR}`echo ${LINE}/`
        echo "${FILESDIR}"

        if [ -d "$FILESDIR" ];then

                FILESCOUNT=`ls -l ${FILESDIR}*.csv | grep -v "total" | wc -l`

                if [ "$FILESCOUNT" -gt 0 ];then

                        FILES=`ls ${FILESDIR}*.csv | xargs`
                        echo "INFO : Process stating on files in ${LINE}"

                        for FILENAME in ${FILES}
                        do

                                echo "INFO : Working on File ${FILENAME} in directory ${LINE}"
                                ## move and Rename file.
                                mv ${FILENAME} ${TEMPDIR}/`echo ${LINE}_temp/`${LINE}_temp.csv

                                ## Verification of csv headers
                                verifyfile ${LINE}_temp.csv ${FILENAME};

                                ## Invoke function to load data to temporory table
                                loadTempTable ${LINE}_temp.csv;

                                ## Update or Insert to master table
                                pushUpdateOrInsert ${LINE}
                        done

                        echo "INFO : Processing on table ${LINE} END"
                else
                        echo "ERROR : There is no files present in ${LINE} directory"
                fi
        else
                echo "ERROR : Table directory ${LINE} is not present in file system"
        fi
fi