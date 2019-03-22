#!/bin/bash

## Processing directory
PRODIR="/tmp/integration/processed"
ERRDIR="/tmp/integration/error/"
LOGDIR="/tmp/integration/logs/"
TEMPDIR="/tmp/integration/temp"
FILESDIR="/tmp/integration/inbound/tmo/case/"

#Declaring mysql DB connection

MASTER_DB_USER='tfbuser'
MASTER_DB_PASSWD='tfbintegration'
MASTER_DB_PORT=3306
MASTER_DB_HOST='tfb-integration.cmvubf6cpu8u.us-west-2.rds.amazonaws.com'
MASTER_DB_NAME='tfbintegration'

SQL_Query='select * from TmpCase limit 10'

Insert_SQL_Query='Insert into case Select * from case_temp where not exists (Select 1 from case where case.id = case_temp.id)'

Update_SQL_Query='Update case set comment='sample comment' where id=1'


function loadTempTable()
{

        echo "inside loadTempTable"
        echo -e "\tLoading .... file `basename $1` to temporory table"

        mysqlimport --ignore-lines=1 --fields-terminated-by="|" --verbose --local --delete --host=tfb-integration.cmvubf6cpu8u.us-west-2.rds.
amazonaws.com --user tfbuser --password=tfbintegration --columns=Id,CreatedDate,LastModifiedDate,AccountId,Origin,Reason,ContactEmail,Contact
Mobile,ContactId,ContactPhone,ClosedDate,Description,Status,Subject,Type,ACK_Form__c,Approval_Status__c,BAN_Number__c,Case_Comment__c,City__c
,Close_Date__c,Country__c,Credit_Class__c,Current_Rate_Plan__c,Customer_Status__c,Date_Approved__c,E_Rate_Case__c,Federal_Tax_ID__c,Feeney_Or
der__c,Inquiry_Type__c,Last_Case_Comment_Date__c,Last_Status_Change__c,Lead_name__c,Mobile_Number__c,MSISDN__c,Notes__c,Number_of_Devices_Aff
ected__c,Number_of_RMA_Devices__c,Opportunity_Stage__c,Product_Needed__c,Reason__c,Reason_For_Escalation__c,Technical_Care_Recommendation__c,
Reason_for_RMA_Exception__c,Region__c,Related_Channel__c,Request_Type__c,Resolution_Category__c,RFP_Due_Date__c,RFP_Format__c,RFP_Type__c,RMA
_number__c,Severity__c,State_Province__c,Street__c,Sub_Type__c,Tax_Exempt_Y_N__c,Title__c,Topic__c,Total_Amount1__c,Total_Opportunity_Lines__
c,Type__c,Type_of_Record__c,User_Segment__c,Win_Loss__c,Zip_Postal_Code__c tfbintegration ${TEMPDIR}/*.csv

         if [ $? != 0 ]
        then
                mv ${TEMPDIR}/*.csv ${ERRDIR}

        fi

         if [ $? == 0 ]
        then
                                mv ${TEMPDIR}/*.csv ${PRODIR}/`basename $1`.csv
                 echo "Success"
        fi

        echo -e "\t  Importing completed on table tmo_case_temp of file `basename $1`..."
}
## End of loadTempTable


function pushUpdateOrInsert()
{
        echo "Pushing data to master table"
mysql -u$MASTER_DB_USER -p$MASTER_DB_PASSWD -P$MASTER_DB_PORT -h$MASTER_DB_HOST -D$MASTER_DB_NAME <<EOF
$Insert_SQL_Query;
$Update_SQL_Query;
EOF

echo "Query Execution Done"

}
## End of pushUpdateOrInsert

## Read csv file names
FILES=`ls ${FILESDIR}*.csv | xargs`

echo ${FILES}

for FILENAME in ${FILES}
do

                echo "Processing File ${FILENAME} ..."
                ## copy and Rename file.
                cp ${FILENAME} ${TEMPDIR}/tmo_case_temp.csv


                ## Clear Temporty table data
                ##clearTempororyTable;

                ## Invoke function to load data to temporory table
                loadTempTable ${FILENAME};

                ## Update or Insert to master table
                pushUpdateOrInsert

done