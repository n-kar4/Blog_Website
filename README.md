#	Description	TTC	"Transaction 
Type"	"Transaction 
Category"	7th character in TransactionCode	10th character in TransactionCode	18th character in TransactionCode	"Last character in TransactionCode
(17th char)"
1	Regular_Payment	R01	A	RECUR	Z	X		<> F,G or H
2	Tax_Free	R03	A	RECUR	n/a	X		= F,G or H
3	Convenience	R04	D	RECUR	n/a	N	<>H	n/a
4	Reimbursement	R05	A	RECUR	R	X		<> F,G or H
5	Special	R06	A	RECUR	S	X		<> F,G or H
								
6	Novated_Regular_Payment	E01	A	RECUR	Z	N		<> F,G or H
7	Novated_Tax_Free	E03	A	RECUR	n/a	N		= F,G or H
8	Novated_Convenience	E04	D	RECUR	n/a	X or 0	<>H	n/a
9	Novated_Reimbursement	E05	A	RECUR	R	N		<> F,G or H
10	Novated_Special	E06	A	RECUR	W or S	N		<> F,G or H
								
12	H Deduction	MH1	D or N	RECUR	n/a	n/a	H	n/a

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

# s3 = boto3.resource('s3')
# #tgt_bucket='uat1-gwf-cc-cats-publish-class0-us-east-1'
tgt_bucket='uat2-gwf-cc-cats-filexfer-us-east-1'
# #tgt_key='release01/2023-12-07/'
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


# Source Data
# ######################################################################################
# payments

print("Source data payment")

#weekly
# PAYMENTS_PATH="s3://uat2-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_PAYMENTS_12062024_104719.csv"

#Monthly
PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/DB_PAYMENT_PAYMENTS_M01_01312025_122503.csv"

PAYMENTS_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{PAYMENTS_PATH}")
PAYMENTS_DF.createOrReplaceTempView("payment")
print(PAYMENTS_DF.printSchema())
# spark.sql("select * from  payment limit 30").show(truncate=0)




# ######################################################################################
# target Data
# ######################################################################################
# preprocessed payments

print("target data payment")

#weekly
# PAYMENTS_PATH="s3://uat2-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_PAYMENTS_12062024_104719.csv"

#Monthly
P_PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_PAYMENTS_M01_01312025_122503.csv"

P_PAYMENTS_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{P_PAYMENTS_PATH}")
P_PAYMENTS_DF.createOrReplaceTempView("preprocessed")
print(P_PAYMENTS_DF.printSchema())
# spark.sql("select * from  preprocessed limit 30").show(truncate=0)
# spark.sql("select * from  preprocessed where PaymentID='c3970c0d-7628-4faa-afd8-7dd9a9ab0758'").show(truncate=0)
# ######################################################################################


print("source join data")

df_src_1=spark.sql('''
    select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding,p.AccountFunding
 from payment p join preprocessed pre on p.payeeid = pre.payeeid and p.paymentid = pre.paymentid and p.TransactionCode=pre.TransactionCode
 where
    trim(p.TransactionType) = 'A' and
    trim(p.TransactionCategory)='RECUR' and
    substr(trim(p.TransactionCode),10,1) = 'X' and
    substr(trim(p.TransactionCode), -1) in ('F', 'G', 'H') and
    split_part(pre.AccountFunding,".", 3) = 'R03'
''')

df_src_1.show(30,truncate=0)
print("F2........End")
print("count from source ="+str(df_src_1.count()))


df_src_2=spark.sql('''
SELECT 
    paymentid, 
    COUNT(DISTINCT AccountFunding) AS distinct_accountfunding_count
FROM
    preprocessed
GROUP BY
    paymentid
HAVING 
    COUNT(DISTINCT AccountFunding) > 1;
''')

df_src_2.show(30,truncate=0)
print("F2........End")
print("count from source ="+str(df_src_2.count()))

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


# Save_PATH ="s3://uat2-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/ERF9_errlog/erf9_src_data/"
# df_src_1.coalesce(1).write.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).mode("overwrite").save(f"{Save_PATH}")




job.commit()
