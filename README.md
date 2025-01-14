import sys
import time
import boto3
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.resource('s3')
#tgt_bucket='uat1-gwf-cc-cats-publish-class0-us-east-1'
tgt_bucket='uat1-gwf-cc-cats-filexfer-us-east-1'
#tgt_key='release01/2023-12-07/'
tgt_key='outbound/CaTS_Automation_Testing/ERF9/'


cur_date=str(time.strftime('%Y%m%d'))
day_of_year=datetime.now().timetuple().tm_yday
year=str(time.strftime('%y'))
date_file=str(time.strftime('%Y%m%d%H%M%S'))

src_bucket = 'uat1-gwf-cc-cats-filexfer-us-east-1'
src_key= 'inbound/telus/'
#########################################
# Monthly Source Files

Monthly_Pregexp = 'P_DB_PAYMENT_PAYMENTS_M\d{2}_\d{8}_\d{6}'
Monthly_Dregexp = 'DB_PAYMENT_DEPOSIT_M\d{2}_\d{8}_\d{6}'
#########################################
# Weekly Source Files

Weekly_Pregexp = 'P_DB_PAYMENT_PAYMENTS_\d{8}_\d{6}'
Weekly_Dregexp = 'DB_PAYMENT_DEPOSIT_\d{8}_\d{6}'


###############################################
#To retrieve latest source Files
def get_latest_file(src_bucket=None, src_key=None, regexp=None):
    if src_bucket and src_key and regexp:
        s3=boto3.client('s3')
        bucket_name=src_bucket
        folder_prefix=src_key
        pattern=regexp
        response=s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        if response is not None and response!='':
            matching_files=[obj['Key'] for obj in response['Contents'] if re.match(pattern,obj['Key'].split('/')[-1])]
            matching_files.sort(key=lambda x: s3.head_object(Bucket=bucket_name,Key=x)['LastModified'],reverse=True)
            latest_file=matching_files[0] if len(matching_files)>0 else None
            #print(latest_file)
            latest_file_path = str(bucket_name+'/'+latest_file)
        else:
            sys.exit(f"File Not found for file pattern {regexpr_pattern_} in folder {key_x} in bucket {bucket_x}")
    return latest_file_path
    
############################################################
#Source File to run Monthly or Weekly 

run = 'Monthly'

if run == 'Weekly':
    #Monthly
    payment_file_path = get_latest_file(src_bucket, src_key, Monthly_Pregexp)
    deposit_file_path = get_latest_file(src_bucket, src_key, Monthly_Dregexp)
else:
    #Weekly
    payment_file_path = get_latest_file(src_bucket, src_key, Weekly_Pregexp)
    deposit_file_path = get_latest_file(src_bucket, src_key, Weekly_Dregexp)


print(payment_file_path)
print(deposit_file_path)
# Source Data
# ######################################################################################
# payments

print("Source data payment")
#PAYMENTS_PATH = 's3://'+payment_file_path

#weekly
PAYMENTS_PATH = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_PAYMENTS_01062025_104719.csv"


#Monthly
# PAYMENTS_PATH = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_PAYMENTS_M12_12082024_125245.csv"

# PAYMENTS_PATH = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_EXCEPTIONS_PAYMENTS_ERR_10252024_124446.csv"
PAYMENTS_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{PAYMENTS_PATH}")
PAYMENTS_DF.createOrReplaceTempView("payment")
print(PAYMENTS_DF.printSchema())
spark.sql("select * from  payment limit 30").show(truncate=0)
# spark.sql("select * from  payment where PayeeID = '10000000001Z16493P01' ").show(truncate=0)

# ######################################################################################
# Lookup (fund gg lookup)

print("Source data Fund GG Lookup")

Cats_Accounting_Map_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/lookup/erf_contracts_with_fund_id_gg_lookup/erf_contracts_w_Fund_ID_with_GG_Lookup.csv"
Cats_Accounting_Map_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{Cats_Accounting_Map_PATH}")
Cats_Accounting_Map_DF.createOrReplaceTempView("fund_lookup")
print(Cats_Accounting_Map_DF.printSchema())
spark.sql("select * from fund_lookup limit 30").show(truncate=0)


# ######################################################################################
# ttc_lookup

print("Source data TTC_lookup")

Inprocess_Cats_Accounting_Map_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/lookup/ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds/ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds.csv"
Inprocess_Cats_Accounting_Map_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter=',', quote='"', escape='"', skip_blank_lines=True).load(f"{Inprocess_Cats_Accounting_Map_PATH}")
Inprocess_Cats_Accounting_Map_DF.createOrReplaceTempView("ttc_lookup")
print(Inprocess_Cats_Accounting_Map_DF.printSchema())
spark.sql("select * from ttc_lookup limit 30").show(truncate=0)


# ######################################################################################
# PAYMENT_DEPOSIT

print("Source data PAYMENT_DEPOSIT")

#PAYMENT_DEPOSIT_PATH ='s3://'+deposit_file_path

#weekly
PAYMENT_DEPOSIT_PATH = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/DB_PAYMENT_DEPOSIT_01072025_104719.csv"

#Monthly
# PAYMENT_DEPOSIT_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/DB_PAYMENT_DEPOSIT_M12_12082024_125245.csv"

# PAYMENT_DEPOSIT_PATH = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/DB_PAYMENT_EXCEPTIONS_DEPOSIT_ERR_10252024_124446.csv"
PAYMENT_DEPOSIT_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{PAYMENT_DEPOSIT_PATH}")
PAYMENT_DEPOSIT_DF.createOrReplaceTempView("deposit")
print(PAYMENT_DEPOSIT_DF.printSchema())
spark.sql("select * from deposit limit 30").show(truncate=0)


# ######################################################################################


print("source join data")


df_src_1 = spark.sql(''' select DISTINCT split_part(p.AccountFunding,".", 1) AS SOURCE_ACCT_CNTRCT_NUM, 
    split_part(p.AccountFunding,".", 2) AS SOURCE_SDIO, 
    split_part(p.AccountFunding,".", 3) AS SOURCE_TTC, 
    TRIM(p.TransactionCategory) AS SOURCE_TRANSACTION_CATEGORY, 
    TRIM(p.TransactionCode) AS SOURCE_TRANSACTION_CODE, 
    l.cntrct_num AS ERF_CNTRCT_NUM, 
    l.CNTRCT_TYP_CD AS ERF_CNTRCT_TYP_CD, 
    l.fund_id AS ERF_FUND_ID, 
    lookup.trans_Typ_Cd AS ERF_TRANS_TYPE_CODE, 
    "Invalid SDIO and Transaction Code combination found in the source file" AS ERROR_DESCRIPTION  from payment p inner join deposit d on p.payeeid = d.payeeid and p.paymentid = d.paymentid join fund_lookup l on SPLIT(p.AccountFunding,'\\\\.')[0] = trim(l.EASY_Group_Account_Id) and SPLIT(p.AccountFunding,'\\\\.')[1] = trim(l.SDIO_ID) inner join ttc_lookup lookup on SPLIT(p.AccountFunding,'\\\\.')[2] = trim(lookup.Telus_Code) and trim(lookup.Telus_Primary_Ind) = 'Y' where trim(p.TransactionType) = 'A' and substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and ((substr(trim(p.TransactionCode),9,1) != 'P' and split_part(p.AccountFunding,'.',2) != 'PURANN')
         or (substr(trim(p.TransactionCode),9,1) != 'P' and split_part(p.AccountFunding,'.',2) = 'PURANN') 
         or (substr(trim(p.TransactionCode),9,1) = 'P' and split_part(p.AccountFunding,'.',2) != 'PURANN'))
    
    union 
    select DISTINCT split_part(p.AccountFunding,".", 1) AS SOURCE_ACCT_CNTRCT_NUM, 
    split_part(p.AccountFunding,".", 2) AS SOURCE_SDIO, 
    split_part(p.AccountFunding,".", 3) AS SOURCE_TTC, 
    TRIM(p.TransactionCategory) AS SOURCE_TRANSACTION_CATEGORY, 
    TRIM(p.TransactionCode) AS SOURCE_TRANSACTION_CODE, 
    l.cntrct_num AS ERF_CNTRCT_NUM, 
    l.CNTRCT_TYP_CD AS ERF_CNTRCT_TYP_CD, 
    l.fund_id AS ERF_FUND_ID, 
    lookup.trans_Typ_Cd AS ERF_TRANS_TYPE_CODE, 
    "Easy Group Account ID to ERF CNTRCT_NUM translation not found in the erf_contracts w Fund ID with GG Lookup file" AS ERROR_DESCRIPTION  from payment p inner join deposit d on p.payeeid = d.payeeid and p.paymentid = d.paymentid join fund_lookup l on SPLIT(p.AccountFunding,'\\\\.')[0] = trim(l.EASY_Group_Account_Id) and SPLIT(p.AccountFunding,'\\\\.')[1] = trim(l.SDIO_ID) join ttc_lookup lookup on SPLIT(p.AccountFunding,'\\\\.')[2] = trim(lookup.Telus_Code) and trim(lookup.Telus_Primary_Ind) = 'Y' where trim(p.TransactionType) = 'A' and substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and (substr(trim(p.TransactionCode),9,1) != 'P' and split_part(p.AccountFunding,'.',2) != 'PURANN') and
    not exists(select 1 from  fund_lookup fl  where fl.EASY_Group_Account_Id = split_part(p.AccountFunding,'.',1))
    
    union
    select DISTINCT split_part(p.AccountFunding,".", 1) AS SOURCE_ACCT_CNTRCT_NUM, 
    split_part(p.AccountFunding,".", 2) AS SOURCE_SDIO, 
    split_part(p.AccountFunding,".", 3) AS SOURCE_TTC, 
    TRIM(p.TransactionCategory) AS SOURCE_TRANSACTION_CATEGORY, 
    TRIM(p.TransactionCode) AS SOURCE_TRANSACTION_CODE, 
    l.cntrct_num AS ERF_CNTRCT_NUM, 
    l.CNTRCT_TYP_CD AS ERF_CNTRCT_TYP_CD, 
    l.fund_id AS ERF_FUND_ID, 
    lookup.trans_Typ_Cd AS ERF_TRANS_TYPE_CODE, 
    "SDIO ID to ERF Fund ID translation not found in the erf_contracts w Fund ID with GG Lookup file" AS ERROR_DESCRIPTION  from payment p inner join deposit d on p.payeeid = d.payeeid and p.paymentid = d.paymentid join fund_lookup l on SPLIT(p.AccountFunding,'\\\\.')[0] = trim(l.EASY_Group_Account_Id) and SPLIT(p.AccountFunding,'\\\\.')[1] = trim(l.SDIO_ID) join ttc_lookup lookup on SPLIT(p.AccountFunding,'\\\\.')[2] = trim(lookup.Telus_Code) and trim(lookup.Telus_Primary_Ind) = 'Y' where trim(p.TransactionType) = 'A' and substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and
        (substr(trim(p.TransactionCode),9,1) != 'P' and split_part(p.AccountFunding,'.',2) != 'PURANN') and 
        not exists(select 1 from  fund_lookup fl  where fl.SDIO_ID = split_part(p.AccountFunding,'.',2))

    
       

    
    union
    select DISTINCT split_part(p.AccountFunding,".", 1) AS SOURCE_ACCT_CNTRCT_NUM, 
    split_part(p.AccountFunding,".", 2) AS SOURCE_SDIO, 
    split_part(p.AccountFunding,".", 3) AS SOURCE_TTC, 
    TRIM(p.TransactionCategory) AS SOURCE_TRANSACTION_CATEGORY, 
    TRIM(p.TransactionCode) AS SOURCE_TRANSACTION_CODE, 
    l.cntrct_num AS ERF_CNTRCT_NUM, 
    l.CNTRCT_TYP_CD AS ERF_CNTRCT_TYP_CD, 
    l.fund_id AS ERF_FUND_ID, 
    lookup.trans_Typ_Cd AS ERF_TRANS_TYPE_CODE, 
    "Telus Code to ERF Trans Type Dode translation not found in the ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes filee" AS ERROR_DESCRIPTION  from payment p inner join deposit d on p.payeeid = d.payeeid and p.paymentid = d.paymentid join fund_lookup l on SPLIT(p.AccountFunding,'\\\\.')[0] = trim(l.EASY_Group_Account_Id) and SPLIT(p.AccountFunding,'\\\\.')[1] = trim(l.SDIO_ID) join ttc_lookup lookup on SPLIT(p.AccountFunding,'\\\\.')[2] = trim(lookup.Telus_Code) and trim(lookup.Telus_Primary_Ind) = 'Y' where trim(p.TransactionType) = 'A' and substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and (substr(trim(p.TransactionCode),9,1) != 'P' and split_part(p.AccountFunding,'.',2) != 'PURANN')  and 
         not exists(select 1 from  ttc_lookup ttc where ttc.telus_code = split_part(p.AccountFunding,'.',3))

    ''')

# df_src_1 = spark.sql(''' select DISTINCT split_part(p.AccountFunding,".", 1) AS SOURCE_ACCT_CNTRCT_NUM, 
#     split_part(p.AccountFunding,".", 2) AS SOURCE_SDIO, 
#     split_part(p.AccountFunding,".", 3) AS SOURCE_TTC, 
#     TRIM(p.TransactionCategory) AS SOURCE_TRANSACTION_CATEGORY, 
#     TRIM(p.TransactionCode) AS SOURCE_TRANSACTION_CODE, 
#     l.cntrct_num AS ERF_CNTRCT_NUM, 
#     l.CNTRCT_TYP_CD AS ERF_CNTRCT_TYP_CD, 
#     l.fund_id AS ERF_FUND_ID, 
#     lookup.trans_Typ_Cd AS ERF_TRANS_TYPE_CODE, 
#     "Invalid SDIO and Transaction Code combination found in the source file" AS ERROR_DESCRIPTION  from payment p inner join deposit d on p.payeeid = d.payeeid and p.paymentid = d.paymentid inner join fund_lookup l on SPLIT(p.AccountFunding,'\\\\.')[0] = trim(l.EASY_Group_Account_Id) and SPLIT(p.AccountFunding,'\\\\.')[1] = trim(l.SDIO_ID) inner join ttc_lookup lookup on SPLIT(p.AccountFunding,'\\\\.')[2] = trim(lookup.Telus_Code) and trim(lookup.Telus_Primary_Ind) = 'Y' where trim(p.TransactionType) = 'A' and substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and ((substr(trim(p.TransactionCode),9,1) != 'P' and split_part(p.AccountFunding,'.',2) != 'PURANN')
#          or (substr(trim(p.TransactionCode),9,1) != 'P' and split_part(p.AccountFunding,'.',2) = 'PURANN') 
#          or (substr(trim(p.TransactionCode),9,1) = 'P' and split_part(p.AccountFunding,'.',2) != 'PURANN'))''')


df_src_1.show(80,truncate=0)
#df_src_1.write.saveAsTable("bb_src_data")
print("F2........End")
print("count from source ="+str(df_src_1.count()))


Save_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/ERF9_errlog/erf9_src_data/"
df_src_1.coalesce(1).write.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).mode("overwrite").save(f"{Save_PATH}")




job.commit()

















##############log file generation by comparison

import sys
import time
import boto3
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ERF detail/trailer record in sep lines
########################################################################3
s3 = boto3.resource('s3')

try:
    # s3_obj = s3.Object(bucket_name = 'uat1-gwf-cc-cats-filexfer-us-east-1', key = 'outbound/erf/weekly/ERF_DB_TRANS') #weekly path
    #s3_obj = s3.Object(bucket_name = 'uat1-gwf-cc-cats-filexfer-us-east-1', key = 'outbound/erf/weekly/ERF_DB_TRANS_EXCEPTION') #monthly path
    # s3_obj = s3.Object(bucket_name = 'uat1-gwf-cc-cats-filexfer-us-east-1', key = 'outbound/erf/monthly/ERF_DB_TRANS') #weekly exception path
    s3_obj = s3.Object(bucket_name = 'uat1-gwf-cc-cats-filexfer-us-east-1', key = 'outbound/erf/monthly/ERF_DB_TRANS_EXCEPTION') #monthly exception path
    #s3_obj = s3.Object(bucket_name = 'uat1-gwf-cc-cats-filexfer-us-east-1', key = 'outbound/erf/weeklyreissue/ERF_DB_TRANS') #weekly reissue path
    s3 = s3_obj.get()
    s3_obj_body = s3.get('Body')
    detail = s3_obj_body.read().decode()
    #print(type(detail))
    print(detail)
    
except s3.meta.client.exception.NoSuchBucket as e:
    print("No such Bucket")
    print(e)
    
except s3.meta.client.exception.NoSuchKey as e:
    print("No such key")
    print(e)

#########################################
try: 
    #one record character 200
    n =200
    out = [(detail[i:i+n]) for i in range(0,len(detail), n)]
    detail_df = pd.DataFrame(out)
    trailer_df = detail_df.tail(1)
    detail_df = detail_df[:-1]
    print(detail_df.shape[0])
    
    detail_df.to_csv("s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/ERF9/tgt_ERF9_File.txt", header = False, index =False, encoding= 'utf-8')
    print("Target split line file created")
    
    trailer_df.to_csv("s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/ERF9/trailer_ERF9_File.txt", header = False, index =False, encoding= 'utf-8')
    print("Trailer split line file created")
    
except Exception as e:
    print("Exception Error:", e)
    

s3uri = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/ERF9/tgt_ERF9_File.txt"

tgt_bucket='uat1-gwf-cc-cats-filexfer-us-east-1'
tgt_key='outbound/CaTS_Automation_Testing/ERF9/'


cur_date=str(time.strftime('%Y%m%d'))
day_of_year=datetime.now().timetuple().tm_yday
year=str(time.strftime('%y'))
date_file=str(time.strftime('%Y%m%d%H%M%S'))


# Source Data
# ######################################################################################
print("Source data")
SRC_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/ERF9/erf9_src_data/*"

SRC_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{SRC_PATH}")
SRC_DF.createOrReplaceTempView("srcdata")

spark.sql("select * from srcdata limit 50").show(truncate=0)

src_recs=str(spark.sql("select count(1) from srcdata").collect()[0][0])
print("Total Source records....."+str(src_recs))

# ######################################################################################

erf9_detail_rec =  {
"PARIS_REC_TYPE":{"testcasenumber":1,"pos":"1-2","len":2,"default":"yes","default_val":"01","testcase":"default value check"}, 
"CNTR_NUM":{"testcasenumber":2,"pos":"3-8","len":6,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"CNTR_TYPE":{"testcasenumber":3,"pos":"9-11","len":3,"default":"no","default_val":"NA","testcase":"Null check ","req":"yes"}, 
"FUND_ID":{"testcasenumber":4,"pos":"12-14","len":3,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"CNTR_SUB":{"testcasenumber":5,"pos":"15-17","len":3,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"PREM_TAX":{"testcasenumber":6,"pos":"18-18","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"TB":{"testcasenumber":7,"pos":"19-20","len":2,"default":"yes","default_val":"s_2","testcase":"default value check"}, 
"TCN":{"testcasenumber":8,"pos":"21-30","len":10,"default":"yes","default_val":"s_10","testcase":"default value check"}, 
"ORIG_TTC":{"testcasenumber":9,"pos":"31-35","len":5,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"SPEC_TTC":{"testcasenumber":10,"pos":"36-40","len":5,"default":"yes","default_val":"s_5","testcase":"default value check"}, 
"FUND_EFF":{"testcasenumber":11,"pos":"41-41","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"ASSET_EFF":{"testcasenumber":12,"pos":"42-42","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"CNTR_DT":{"testcasenumber":13,"pos":"43-52","len":10,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"VCH_DT":{"testcasenumber":14,"pos":"53-62","len":10,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"SOURCE_SYSTEM":{"testcasenumber":15,"pos":"63-68","len":6,"default":"yes","default_val":"s_6","testcase":"default value check"}, 
"GEN_YEAR":{"testcasenumber":16,"pos":"69-72","len":4,"default":"yes","default_val":"s_4","testcase":"default value check"}, 
"GEN_PERIOD":{"testcasenumber":17,"pos":"73-73","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"BUS_TYPE":{"testcasenumber":18,"pos":"74-79","len":6,"default":"yes","default_val":"s_6","testcase":"default value check"}, 
"SRVC_AGREE":{"testcasenumber":19,"pos":"80-81","len":2,"default":"yes","default_val":"s_2","testcase":"default value check"}, 
"GEN_CLASS":{"testcasenumber":20,"pos":"82-84","len":3,"default":"yes","default_val":"s_3","testcase":"default value check"}, 
"TRANS_AMOUNT":{"testcasenumber":21,"pos":"85-100","len":16,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"LEGACY_CNTR":{"testcasenumber":22,"pos":"101-107","len":7,"default":"yes","default_val":"s_7","testcase":"default value check"}, 
"LEGACY_SUB_FUND":{"testcasenumber":23,"pos":"108-109","len":2,"default":"yes","default_val":"s_2","testcase":"default value check"}, 
"LEGACY_FUND":{"testcasenumber":24,"pos":"110-116","len":7,"default":"yes","default_val":"s_7","testcase":"default value check"}, 
"NESTED_FUND_ID":{"testcasenumber":25,"pos":"117-119","len":3,"default":"yes","default_val":"s_3","testcase":"default value check"}, 
"DTR_TTC":{"testcasenumber":26,"pos":"120-124","len":5,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"FILLER":{"testcasenumber":27,"pos":"125-200","len":76,"default":"yes","default_val":"s_76","testcase":"default value check"}
}

erf9_trailer_rec = {
"WS_TRL_REC_TYPE":{"testcasenumber":1,"pos":"1-2","len":2,"default":"yes","default_val":"TR","testcase":"default value check"}, 
"WS_TRL_ROW_COUNT":{"testcasenumber":2,"pos":"3-8","len":6,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"WS_TRL_HASH_TOTAL":{"testcasenumber":3,"pos":"9-24","len":16,"default":"yes","default_val":"s_16","testcase":"default value check"}, 
"WS_TRL_TOT_TRANS_AMT":{"testcasenumber":4,"pos":"25-43","len":19,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"FILLER":{"testcasenumber":5,"pos":"44-200","len":157,"default":"yes","default_val":"s_157","testcase":"default value check"}
}


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

def generate_custom_grok_pattern(rec):
    try:
        customPatterns = ""
        
        for column in rec: 
            customPatterns+="(?<"+column+">.{"+str(rec[column]['len'])+"})"
            
        print(customPatterns)
        return customPatterns
    except Exception as e:
        print("Exception raised in generate_custom_grok_pattern()", e)
        raise e
 
rectype ='d'
rec_val = '01'

if rectype == 'd':
    logFormat = generate_custom_grok_pattern(erf9_detail_rec)
    jsondata = erf9_detail_rec
    print(jsondata)
    rec_val = '01'
if rectype == 't':
    logFormat = generate_custom_grok_pattern(erf9_trailer_rec)
    jsondata = erf9_trailer_rec
    rec_val = 'TR'


def read_grok_parsed_dynamic_frame(glueContext, s3uri, logFormat):
    try:
        grokDynamicFrame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [s3uri]},
            format="GrokLog",
            format_options={
                "withHeader":True,
                "logFormat":logFormat}
        )
        return grokDynamicFrame
    
    except Exception as e:
        print("Exception raised in read_grok_parsed_dynamic_frame()", e)
        raise e


grokglueDynamicFrame = read_grok_parsed_dynamic_frame(glueContext, s3uri, logFormat)

print("rec_val......."+str(rec_val))

grokglueDynamicFrame.show()        
sourceDF1 = grokglueDynamicFrame.toDF().repartition(1)
print(sourceDF1.printSchema())
#source_dynamic_frame = DynamicFrame.fromDF(sourceDF, glueContext, "source_dynamic_frame")
#sourceDF.filter(sourceDF[0] == rec_val).show()

if rectype == 'd':
    detailed_rec_cnt = sourceDF1.filter((sourceDF1.PARIS_REC_TYPE == rec_val)).count()
    sourceDF = sourceDF1.filter((sourceDF1.PARIS_REC_TYPE == rec_val))
    sourceDF.cache()
    sourceDF.show(truncate=0)
    
if rectype == 't':
    detailed_rec_cnt = sourceDF1.filter((sourceDF1.WS_TRL_REC_TYPE == rec_val)).count()
    sourceDF = sourceDF1.filter((sourceDF1.WS_TRL_REC_TYPE == rec_val))
    sourceDF.cache()
    sourceDF.show(truncate=0)
    

print("\n Target Data.......")

def return_testcase_res(test_case_number,totalrecs,columnname,pos,defaultvalues,passedrecs,failedrecs,teststatus,testcase):
    return str("{:<20}".format(test_case_number)+"|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(columnname)+"|"+"{:<10}".format(pos)+"|"+"{:<30}".format(str(defaultvalues))+"|"+"{:<20}".format(str(passedrecs))+"|"+"{:<20}".format(failedrecs)+"|"+"{:<10}".format(teststatus)+"|"+"{:<200}".format(testcase))

def apply_tests(row):
    try:
        test_res=""
        for c in erf9_detail_rec: 
            if (erf9_detail_rec[c]['default'] == 'yes'):
                if (row[c] == erf9_detail_rec[c]['default_val']):
                    test_res+="P;"
                else:
                    test_res+="F;"    
        return row + (test_res,)
    
    except Exception as e:
        print("Exception raised in read_grok_parsed_dynamic_frame()", e)
        raise e

#sourceDF.rdd.map(apply_tests).toDF(sourceDF.columns + ["result"]).show(5)

#Target temp table
sourceDF.createOrReplaceTempView("tgtdata")

totalrecs=str(spark.sql("select count(1) from tgtdata").collect()[0][0])

print("Total target records....."+str(totalrecs))


fwf_data=[]
error_data=[]
header_txt = str("{:<20}".format("Testcase_number")+"|"+"{:<20}".format("Total_Records_Count")+"|"+"{:<40}".format("Column")+"|"+"{:<10}".format("Pos")+"|"+"{:<30}".format("Default_values")+"|"+"{:<20}".format("Passed_record_count")+"|"+"{:<20}".format("Failed_record_count")+"|"+"{:<10}".format("Test_Status") +"|"+"Test_Case")
fwf_data.append(header_txt)

error_recs = "Testcase_number|CNTR_NUM|CNTR_TYPE|FUND_ID|CNTR_SUB|Value_in_File|Field"

error_data.append(error_recs)


for c in jsondata:
    
    if (jsondata[c]['default'] == 'yes'):
            test_case=jsondata[c]['testcase']
            test_case_number=str(jsondata[c]['testcasenumber'])
            # default values
            lst = jsondata[c]['default_val'].split('|')
            lst_all_val = []
            for s in lst:
                if 's_' in s:
                    e_str=''
                    for i in range(0,int(s.replace('s_',''))):
                        e_str+=' '    
                    lst_all_val.append(e_str)    
                elif 'z_' in s:        
                    z_str=''
                    for i in range(0,int(s.replace('z_',''))):
                        z_str+='0'    
                    lst_all_val.append(z_str)   
                else:
                    lst_all_val.append(s)    
            
            all_vals = ''
            all_vals =','.join(lst_all_val)
            
                
            print ("select count(1) from tgtdata where "+ c +" in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")) 
            print ("select count(1) from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')"))
            
            passedrecs=str(spark.sql("select count(1) from tgtdata where "+ c +" in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")).collect()[0][0])       
            failedrecs=str(spark.sql("select count(1) from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")).collect()[0][0])
            
            print("Passed records:"+passedrecs)
            
            print("Failed records:"+failedrecs)
            
            teststatus =""
            
            if (int(failedrecs)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(failedrecs)>0):
                
                if rectype == 'h' or rectype == 't':  
                    error_data.extend(spark.sql("select concat("+test_case_number+",'|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")+" limit 100" ).rdd.flatMap(lambda x:x).collect())
                    
                    spark.sql("select concat("+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")).show(truncate=0)
                    
                if rectype == 'd':  
                    error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM, '|', CNTR_TYPE, '|', CNTR_SUB, '|', FUND_ID, '|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")+" limit 100").rdd.flatMap(lambda x:x).collect())
                
                    #spark.sql("select concat(Shareholder_Account_Number,'|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")).show(truncate=0)
                
            txt = str("{:<20}".format(test_case_number)+ "|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(c)+"|"+"{:<10}".format(jsondata[c]['pos'])+"|"+"{:<30}".format(str(jsondata[c]['default_val']))+ "|"+"{:<20}".format(passedrecs)+"|"+"{:<20}".format(failedrecs) +"|"+"{:<10}".format(teststatus) + "|"+"{:<200}".format(test_case))
            fwf_data.append(txt)    
    try:
        if (jsondata[c]['default_val'] == 'NA' and jsondata[c]['req'] == 'yes'):
            test_case=jsondata[c]['testcase']
            #test_case= "Null check"
            test_case_number=str(jsondata[c]['testcasenumber'])
            
            print ("select count(1) from tgtdata where "+ c +" is null or trim("+ c +") = '' ".format(c))
            
            test_case_count = str(spark.sql('''select count(1) from tgtdata where "+ c +" is null or trim("+ c +") = '' '''.format(c)).collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
                
            if (int(test_case_count)>0):
                error_data.extend(spark.sql("select concat("+test_case_number+", '|', CNTR_NUM, '|', CNTR_TYPE, '|', CNTR_SUB, '|', FUND_ID, '|', "+c+", '|', '"+c+"' ) as Res from tgtdata where "+ c +" is null or trim("+ c +") = '' ".format(c)+" limit 100").rdd.flatMap(lambda x:x).collect())
        
            txt = str("{:<20}".format(test_case_number)+ "|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(c)+"|"+"{:<10}".format(jsondata[c]['pos'])+"|"+"{:<30}".format(str(jsondata[c]['default_val']))+ "|"+"{:<20}".format(passed_recs)+"|"+"{:<20}".format(test_case_count) +"|"+"{:<10}".format(teststatus) + "|"+"{:<200}".format(test_case))
            fwf_data.append(txt)
                
            

    except Exception as e:
        print("Exception Error:", e)
    
    # check source to target missing payeeid
    if (c =='TRANS_AMOUNT'):
        
        test_case_number='28'
        print("TRANS_AMOUNT source to target data validation")
        spark.sql('''select distinct t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID,t.ORIG_TTC, t.CNTR_DT from tgtdata t
                    inner join srcdata s 
                    on trim(s.CNTRCT_NUM) = trim(t.CNTR_NUM) and trim(s.CNTRCT_TYP_CD) = trim(t.CNTR_TYPE) and trim(s.PRU_Contract_SUB) = trim(t.CNTR_SUB) and trim(s.Fund_ID) = trim(t.FUND_ID) and trim(s.orig_ttc) =  trim(t.ORIG_TTC) and trim(s.PaymentDate) = trim(t.CNTR_DT)
					where cast(t.TRANS_AMOUNT as decimal(16,2)) <> cast(s.trans_amount as decimal(16,2))
					''').show(truncate=0)
        
        test_case_count=str(spark.sql('''select count(1) from (select distinct t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID,t.ORIG_TTC, t.CNTR_DT from tgtdata t
                                        inner join srcdata s 
                                        on trim(s.CNTRCT_NUM) = trim(t.CNTR_NUM) 
                                        and trim(s.CNTRCT_TYP_CD) = trim(t.CNTR_TYPE) 
                                        and trim(s.PRU_Contract_SUB) = trim(t.CNTR_SUB) 
                                        and trim(s.Fund_ID) = trim(t.FUND_ID) 
                                        and trim(s.orig_ttc) =  trim(t.ORIG_TTC) 
                                        and trim(s.PaymentDate) = trim(t.CNTR_DT)
					                    where cast(t.TRANS_AMOUNT as decimal(16,2)) <> cast(s.trans_amount as decimal(16,2)) ) ''').collect()[0][0])
        
        passed_recs=int(totalrecs)-int(test_case_count)
        
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        if (int(test_case_count)>0):
            
            #print("source to target missing payeeid")
            error_data.extend(spark.sql("select concat("+test_case_number+", '|', CNTR_NUM, '|', CNTR_TYPE, '|', CNTR_SUB, '|', FUND_ID, '|', "+c+", '|', '"+c+"'                         ) as Res from tgtdata inner join srcdata s on trim(s.CNTRCT_NUM) = trim(CNTR_NUM) and trim(s.CNTRCT_TYP_CD) = trim(CNTR_TYPE) and trim(s.PRU_Contract_SUB) = trim(CNTR_SUB) and trim(s.Fund_ID) = trim(FUND_ID) and trim(s.orig_ttc) =  trim(ORIG_TTC) and trim(s.PaymentDate) = trim(CNTR_DT) where cast("+ c +" as decimal(16,2)) <> cast(s.trans_amount as decimal(16,2)) ".format(c)+ "limit 100").rdd.flatMap(lambda x:x).collect())
            
            spark.sql('''select distinct t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID,t.ORIG_TTC, t.CNTR_DT from tgtdata t
                                        inner join srcdata s 
                                        on trim(s.CNTRCT_NUM) = trim(t.CNTR_NUM) 
                                        and trim(s.CNTRCT_TYP_CD) = trim(t.CNTR_TYPE) 
                                        and trim(s.PRU_Contract_SUB) = trim(t.CNTR_SUB) 
                                        and trim(s.Fund_ID) = trim(t.FUND_ID) 
                                        and trim(s.orig_ttc) =  trim(t.ORIG_TTC) 
                                        and trim(s.PaymentDate) = trim(t.CNTR_DT)
					                    where cast(t.TRANS_AMOUNT as decimal(16,2)) <> cast(s.trans_amount as decimal(16,2)) ''').show(truncate=0)
                
        txt = return_testcase_res(test_case_number,totalrecs,c,'85-100','NA',str(passed_recs),str(test_case_count),teststatus,'TRANS_AMOUNT source to target data validation')
        
        fwf_data.append(txt)   
        
    
    if (c == 'WS_TRL_ROW_COUNT'):
        
        test_case_number='29'
        
        print("WS_TRL_ROW_COUNT detail count check")
        spark.sql('''with detail_record_count as (select count(1) as tgt_cnt from srcdata) select detail_record_count.tgt_cnt from detail_record_count ''').show(truncate=0)
        
        test_case_count=str(spark.sql('''with detail_record_count as (select count(1) as tgt_cnt from srcdata) select detail_record_count.tgt_cnt - cast                                         (ws_trl_row_count as int) from  detail_record_count,tgtdata  ''').collect()[0][0])
        
        passed_recs=int(totalrecs)-int(test_case_count)
        
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        if (int(test_case_count)>0):
            
            error_data.extend(spark.sql("select concat("+test_case_number+", '|', "+c+", '|', '"+c+"' ) as Res from tgtdata where "+ c +" is not null ".format(c)+ "limit 100").rdd.flatMap(lambda x:x).collect())
            
            spark.sql('''with detail_record_count as (select count(1) as tgt_cnt from srcdata) select detail_record_count.tgt_cnt - cast                                         (ws_trl_row_count as int) from  detail_record_count,tgtdata ''').show(truncate=0)
                
        txt = return_testcase_res(test_case_number,totalrecs,c,'3-8','NA',str(passed_recs),str(test_case_count),teststatus,'count of field should match with number of records in target file')
        
        fwf_data.append(txt)  
        
        
    if (c == 'WS_TRL_TOT_TRANS_AMT'):
        
        test_case_number='30'
        
        print("WS_TRL_TOT_TRANS_AMT detail count check")
        spark.sql('''with detail_tot_amt as (select sum(cast (trans_amount as decimal(16, 2))) as tgt_amt from srcdata) select case when detail_tot_amt.tgt_amt - cast(ws_trl_tot_trans_amt as decimal(16, 2)) <> 0 then 1 else 0 end from detail_tot_amt,tgtdata''').show(truncate=0)
        
        test_case_count=str(spark.sql('''with detail_tot_amt as (select sum(cast (trans_amount as decimal(16, 2))) as tgt_amt from srcdata) select case when detail_tot_amt.tgt_amt - cast(ws_trl_tot_trans_amt as decimal(16, 2)) <> 0 then 1 else 0 end from detail_tot_amt,tgtdata ''').collect()[0][0])
        
        passed_recs=int(totalrecs)-int(test_case_count)
        
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        if (int(test_case_count)>0):
            
            error_data.extend(spark.sql("select concat("+test_case_number+", '|', "+c+", '|', '"+c+"' ) as Res from tgtdata where "+ c +" is not null ".format(c)+ "limit 100").rdd.flatMap(lambda x:x).collect())
            
            spark.sql('''with detail_tot_amt as (select sum(cast (trans_amount as decimal(16, 2))) as tgt_amt from srcdata) select detail_tot_amt.tgt_amt from detail_tot_amt''').show(truncate=0)
                
        txt = return_testcase_res(test_case_number,totalrecs,c,'25-43','NA',str(passed_recs),str(test_case_count),teststatus,'Transaction Amount Summed in target File')
        
        fwf_data.append(txt)
        
        
    if (c == 'TRANS_AMOUNT'):
        
        test_case_number='31'
        
        print("Data Count Validation between source and Target")
        spark.sql('''select distinct trim(s.CNTRCT_NUM), trim(s.CNTRCT_TYP_CD), trim(s.PRU_Contract_SUB), trim(s.Fund_ID), trim(s.orig_ttc), trim(s.orig_ttc),            trim(s.PaymentDate), trim(s.PaymentDate), cast(s.trans_amount as decimal(16,2)) from srcdata s
                    except
                    select distinct trim(t.CNTR_NUM), trim(t.CNTR_TYPE), trim(t.CNTR_SUB), trim(t.FUND_ID), trim(t.ORIG_TTC), trim(t.DTR_TTC), trim(t.CNTR_DT),
                    trim(t.VCH_DT), cast(t.TRANS_AMOUNT as decimal(16,2)) from tgtdata t  ''').show(truncate=0)
        
        test_case_count=str(spark.sql('''select count(1) from (select distinct trim(s.CNTRCT_NUM), trim(s.CNTRCT_TYP_CD), trim(s.PRU_Contract_SUB), trim(s.Fund_ID                                 ), trim(s.orig_ttc),  trim(s.orig_ttc), trim(s.PaymentDate), trim(s.PaymentDate), 
                                            cast(s.trans_amount as decimal(16,2)) from srcdata s
                                            except
                                            select distinct trim(t.CNTR_NUM), trim(t.CNTR_TYPE), trim(t.CNTR_SUB), trim(t.FUND_ID), trim(t.ORIG_TTC), 
                                            trim(t.DTR_TTC), trim(t.CNTR_DT),trim(t.VCH_DT), cast(t.TRANS_AMOUNT as decimal(16,2)) from tgtdata t) ''').collect()[0][0])
        
        passed_recs=int(totalrecs)-int(test_case_count)
        
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        if (int(test_case_count)>0):
            
            #print("source to target missing payeeid")
            
            spark.sql('''select distinct trim(s.CNTRCT_NUM), trim(s.CNTRCT_TYP_CD), trim(s.PRU_Contract_SUB), trim(s.Fund_ID), trim(s.orig_ttc), trim(s.orig_ttc),            trim(s.PaymentDate), trim(s.PaymentDate), cast(s.trans_amount as decimal(16,2)) from srcdata s
                            except
                            select distinct trim(t.CNTR_NUM), trim(t.CNTR_TYPE), trim(t.CNTR_SUB), trim(t.FUND_ID), trim(t.ORIG_TTC), trim(t.DTR_TTC), trim(t.CNTR_DT),trim(t.VCH_DT), cast(t.TRANS_AMOUNT as decimal(16,2)) from tgtdata t  ''').show(truncate=0)
                
        txt = return_testcase_res(test_case_number,totalrecs,c,'85-100','NA',str(passed_recs),str(test_case_count),teststatus,'Data Count Validation between source and Target')
        
        fwf_data.append(txt)
        
        
    if (c == 'CNTR_NUM'):
        
        test_case_number='32'
        
        print("Duplicate Check")
        spark.sql('''select t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID, t.ORIG_TTC, t.DTR_TTC, t.CNTR_DT,
                    t.VCH_DT from tgtdata t group by t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID, t.ORIG_TTC, t.DTR_TTC, t.CNTR_DT,
                    t.VCH_DT having count(1) > 1 ''').show(truncate=0)
        
        test_case_count=str(spark.sql('''select count(1) from (select t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID, t.ORIG_TTC, t.DTR_TTC, t.CNTR_DT,
                    t.VCH_DT from tgtdata t group by t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID, t.ORIG_TTC, t.DTR_TTC, t.CNTR_DT,
                    t.VCH_DT having count(1) > 1)''').collect()[0][0])
        
        passed_recs=int(totalrecs)-int(test_case_count)
        
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        if (int(test_case_count)>0):
            
            error_data.extend(spark.sql("select concat("+test_case_number+", '|', CNTR_NUM, '|', CNTR_TYPE, '|', CNTR_SUB, '|', FUND_ID, '|', "+c+", '|', '"+c+"'                         ) as Res from tgtdata group by "+ c +" , CNTR_TYPE, CNTR_SUB, FUND_ID, ORIG_TTC, DTR_TTC, CNTR_DT, VCH_DT having count(1) > 1 ".format(c)+ "limit 100").rdd.flatMap(lambda x:x).collect())
            
            spark.sql('''select t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID, t.ORIG_TTC, t.DTR_TTC, t.CNTR_DT,
                    t.VCH_DT from tgtdata t group by t.CNTR_NUM, t.CNTR_TYPE, t.CNTR_SUB, t.FUND_ID, t.ORIG_TTC, t.DTR_TTC, t.CNTR_DT,
                    t.VCH_DT having count(1) > 1 ''').show(truncate=0)
                
        txt = return_testcase_res(test_case_number,totalrecs,c,'85-100','NA',str(passed_recs),str(test_case_count),teststatus,'Duplicate Check')
        
        fwf_data.append(txt)

    '''
    # check source to target AMOUNT_TOTAL checks on debits
    if (c =='AMOUNT_TOTAL'):
        
        test_case_number='253'
        print("check source to target AMOUNT_TOTAL checks on debits")
        #spark.sql(select distinct s.payeeid, s.paymentid, s.transactioncode, s.AMOUNT_TOTAL, s.dr_cr_ind
                    from srcdata s join 
				        tgtdata t 
					on 
					  trim(t.ZPayeeID) = trim(s.payeeid)
					  and trim(t.ZPaymentID) = trim(s.paymentid)
					  and trim(t.ZTranCdID) = trim(s.transactioncode)
					  and trim(s.dr_cr_ind) = 'D'
					  and trim(t.VGPART3) = ''
					  #and s.VGPART3 is null ).show(truncate=0)
        
        #test_case_count=str(spark.sql(select count(case when trim(t.AMOUNT_TOTAL) <> trim(s.AMOUNT_TOTAL) then 1 else 0 end)
                    from srcdata s join 
				        tgtdata t 
					on 
					  trim(t.ZPayeeID) = trim(s.payeeid)
					  and trim(t.ZPaymentID) = trim(s.paymentid)
					  and trim(t.ZTranCdID) = trim(s.transactioncode)
				  	  and trim(s.dr_cr_ind) = 'D'
					  and trim(t.VGPART3) = ''
					  #and s.VGPART3 is null ).collect()[0][0])
        
        passed_recs=int(totalrecs)-int(test_case_count)
        
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        if (int(test_case_count)>0):
            
            print("check source to target AMOUNT_TOTAL checks on debits")
            
            #spark.sql(select s.payeeid, s.paymentid, s.transactioncode, s.AMOUNT_TOTAL as src_AMOUNT_TOTAL,t.AMOUNT_TOTAL as tgt_AMOUNT_TOTAL, s.dr_cr_ind
                    from srcdata s join 
				        tgtdata t 
					on 
					  trim(t.ZPayeeID) = trim(s.payeeid)
					  and trim(t.ZPaymentID) = trim(s.paymentid)
					  and trim(t.ZTranCdID) = trim(s.transactioncode)
				  	  and trim(s.dr_cr_ind) = 'D'
					  and trim(t.VGPART3) = ''
					  and s.VGPART3 is null
					  #where trim(s.AMOUNT_TOTAL) <> trim(t.AMOUNT_TOTAL)).show(truncate=0)
                
        txt = return_testcase_res(test_case_number,totalrecs,c,'103-118','NA',str(passed_recs),str(test_case_count),teststatus,'check source to target AMOUNT_TOTAL checks on debits')
        
        fwf_data.append(txt)
        '''
try:
    
    print("saving......")        
    
    s3Client=boto3.client('s3')


    if rectype == 'd':
        print("save details records")
        
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF9_Detail_Record_TestResult_{date_file}.TXT')
        
        print("save details records end")
        
        if len(error_data) > 0:
            
            print("details records errors start here")
            print(error_data)
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF9_Detail_Record_TestResult_Failures_{date_file}.TXT')
        
            print("details records errors end here")
            
    if rectype == 't':
        print(type(fwf_data))
        print(fwf_data)
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF9_Trailer_Record_RestResult_{date_file}.TXT')
        
        if len(error_data) > 0:
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF9_Trailer_Record_TestResult_Failure_Recs_{date_file}.txt')
        
        
except Exception as e:
    print("Excelption Error:", e)


job.commit()
