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

#Automation testexecution results------------------------------
tgt_bucket='uat1-gwf-cc-cats-filexfer-us-east-1'
# tgt_bucket='prod-gwf-cc-cats-filexfer-us-east-1'
tgt_key='outbound/CaTS_Automation_Testing/Preprocessor/'


cur_date=str(time.strftime('%Y%m%d'))
day_of_year=datetime.now().timetuple().tm_yday
year=str(time.strftime('%y'))
date_file=str(time.strftime('%Y%m%d%H%M%S'))

src_bucket = 'uat1-gwf-cc-cats-filexfer-us-east-1'
src_key= 'inbound/telus/'



# Source Data
# ######################################################################################
# payments

print("Source data payment")

#weekly
PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_PAYMENTS_02112025_204420.csv"

#Monthly
# PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/DB_PAYMENT_PAYMENTS_M01_01312025_122503.csv"
# PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/archive/2025-02-11/SAP17_Telus_2_SAP/Emergency_Payments/Run_no2/DB_PAYMENT_PAYMENTS_EP_02112025_122503.csv"

PAYMENTS_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{PAYMENTS_PATH}")
PAYMENTS_DF.createOrReplaceTempView("payment")
print(PAYMENTS_DF.printSchema())
print("payment data count: ", PAYMENTS_DF.count())

# spark.sql("select * from  payment limit 30").show(truncate=0)




# ######################################################################################
# target Data
# ######################################################################################
# preprocessed payments

print("target data payment")

#weekly
P_PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/DB_PAYMENT_PAYMENTS_02112025_204420.csv"

#Monthly
# P_PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/telus/P_DB_PAYMENT_PAYMENTS_M01_01312025_122503.csv"
# P_PAYMENTS_PATH="s3://uat1-gwf-cc-cats-filexfer-us-east-1/archive/2025-02-11/SAP17_Telus_2_SAP/Emergency_Payments/Run_no2/P_DB_PAYMENT_PAYMENTS_EP_02112025_122503.csv"


P_PAYMENTS_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{P_PAYMENTS_PATH}")
P_PAYMENTS_DF.createOrReplaceTempView("preprocessed")
print(P_PAYMENTS_DF.printSchema())
print("p_payment data count: ", P_PAYMENTS_DF.count())

# spark.sql("select * from  preprocessed limit 30").show(truncate=0)
# ######################################################################################

# testcases for weekly data

trans_detail_rec_w={
    "Regular_Payment": {"testcasenumber": 1, "TTC": "R51", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Tax_Free": {"testcasenumber": 2, "TTC": "R53", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H"},
    "Convenience": {"testcasenumber": 3, "TTC": "R54", "Transaction Type": "D", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "N", "18th character in TransactionCode": "<>H", "Last character in TransactionCode (17th char)": "n/a"},
    "Reimbursement": {"testcasenumber": 4, "TTC": "R55", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "R", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Special": {"testcasenumber": 5, "TTC": "R56", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "S", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Novated_Regular_Payment": {"testcasenumber": 6, "TTC": "E51", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Novated_Tax_Free": {"testcasenumber": 7, "TTC": "E53", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H"},
    "Novated_Convenience": {"testcasenumber": 8, "TTC": "E54", "Transaction Type": "D", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "X or 0", "18th character in TransactionCode": "<>H", "Last character in TransactionCode (17th char)": "n/a"},
    "Novated_Reimbursement": {"testcasenumber": 9, "TTC": "E55", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "R", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Novated_Special": {"testcasenumber": 10, "TTC": "E56", "Transaction Type": "A", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "W or S", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "H_Deduction": {"testcasenumber": 11, "TTC": "MH1", "Transaction Type": "D or N", "Transaction Category": "<> RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "n/a", "18th character in TransactionCode": "H", "Last character in TransactionCode (17th char)": "n/a"}
}

# testcases for monthly data
trans_detail_rec_m = {
    "Regular_Payment": {"testcasenumber": 1, "TTC": "R01", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Tax_Free": {"testcasenumber": 2, "TTC": "R03", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H"},
    "Convenience": {"testcasenumber": 3, "TTC": "R04", "Transaction Type": "D", "Transaction Category": "RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "N", "18th character in TransactionCode": "<>H", "Last character in TransactionCode (17th char)": "n/a"},
    "Reimbursement": {"testcasenumber": 4, "TTC": "R05", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "R", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Special": {"testcasenumber": 5, "TTC": "R06", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "S", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Novated_Regular_Payment": {"testcasenumber": 6, "TTC": "E01", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Novated_Tax_Free": {"testcasenumber": 7, "TTC": "E03", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H"},
    "Novated_Convenience": {"testcasenumber": 8, "TTC": "E04", "Transaction Type": "D", "Transaction Category": "RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "X or 0", "18th character in TransactionCode": "<>H", "Last character in TransactionCode (17th char)": "n/a"},
    "Novated_Reimbursement": {"testcasenumber": 9, "TTC": "E05", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "R", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "Novated_Special": {"testcasenumber": 10, "TTC": "E06", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "W or S", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H"},
    "H_Deduction": {"testcasenumber": 11, "TTC": "MH1", "Transaction Type": "D or N", "Transaction Category": "RECUR", "7th character in TransactionCode": "n/a", "10th character in TransactionCode": "n/a", "18th character in TransactionCode": "H", "Last character in TransactionCode (17th char)": "n/a"}
}




fwf_data=[]
error_data=[]
header_txt = str("{:<20}".format("Testcase_number")+"|"+"{:<20}".format("Total_Records_Count")+"|"+"{:<40}".format("Column")+"|"+"{:<10}".format("Pos")+"|"+"{:<30}".format("Default_values")+"|"+"{:<20}".format("Passed_record_count")+"|"+"{:<20}".format("Failed_record_count")+"|"+"{:<10}".format("Test_Status") +"|"+"Test_Case")
fwf_data.append(header_txt)

error_recs = "Testcase_number|PayeeID|PaymentID|TransactionCategory|TransactionType|TcCode|p_TcCode|AccountFunding|p_AccountFunding|TransactionAmount"
error_data.append(error_recs)


def return_testcase_res(test_case_number, totalrecs, column, pos, default_values, passed_recs, failed_recs, teststatus, test_case):
    return str("{:<20}".format(test_case_number) + "|" + "{:<20}".format(totalrecs) + "|" + "{:<40}".format(column) + "|" + "{:<10}".format(pos) + "|" + "{:<30}".format(default_values) + "|" + "{:<20}".format(passed_recs) + "|" + "{:<20}".format(failed_recs) + "|" + "{:<10}".format(teststatus) + "|" + test_case)


total_recs=P_PAYMENTS_DF.count()
# total_recs = spark.sql(f'''select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding,p.AccountFundingfrom payment p join preprocessed pre on trim(p.payeeid) = trim(pre.payeeid) and trim(p.paymentid) = trim(pre.paymentid) and trim(p.TransactionCode)=trim(pre.TransactionCode)''').count()
print(total_recs)
        
dfmiss=spark.sql(f'''
    SELECT 
        pre.PaymentID, 
        pre.TransactionCategory, 
        pre.TransactionType, 
        pre.AccountFunding AS src_funding, 
        p.TransactionCode, 
        p.AccountFunding AS tgt_funding
    FROM 
        preprocessed pre
    LEFT JOIN 
        payment p 
    ON 
        pre.payeeid = p.payeeid 
        AND pre.paymentid = p.paymentid 
        AND pre.TransactionCode = p.TransactionCode
    WHERE 
        p.payeeid IS NULL
        OR p.paymentid IS NULL
        OR p.TransactionCode IS NULL''')
print("dfmiss coubnt ",dfmiss.count())
dfmiss.show(30,truncate=0)

if dfmiss.count()>0:
    test_case_number=12
    testcase="source to target data validation"
    
    error_data.extend(
    spark.sql(f'''
                SELECT 
            CONCAT(
                {test_case_number}, '|', 
                COALESCE(pre.PayeeID, ''), '|', 
                COALESCE(pre.PaymentID, ''), '|', 
                COALESCE(pre.TransactionCategory, ''), '|', 
                COALESCE(pre.TransactionType, ''), '|', 
                COALESCE(pre.TransactionCode, ''), '|', 
                COALESCE(p.TransactionCode, '') ,'|',
                COALESCE(pre.AccountFunding, ''), '|', 
                COALESCE(p.AccountFunding, ''), '|',
                COALESCE(p.TransactionAmount, '')
            ) AS Res 
        FROM 
            preprocessed pre 
        LEFT JOIN 
            payment p 
        ON 
            pre.payeeid = p.payeeid 
            AND pre.paymentid = p.paymentid 
            AND pre.TransactionCode = p.TransactionCode
        WHERE 
            p.payeeid IS NULL
            OR p.paymentid IS NULL
            OR p.TransactionCode IS NULL
    ''').rdd.flatMap(lambda x: x).collect())
    test_case_count = str(dfmiss.count())
    passed_recs=int(total_recs)-int(test_case_count)
    print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
    if (int(test_case_count)>0):
        teststatus = "Fail"
    else:
        teststatus = "Pass"
    txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
    fwf_data.append(txt)
    
    
# target null accuntfunding check
if True:
    test_case_number=13
    testcase="null value check at accountfunding in the target data"
    dfmiss=spark.sql(f'''
        SELECT
            pre.PaymentID,
            pre.TransactionCategory,
            pre.TransactionType,
            pre.TransactionCode,
            pre.AccountFunding AS src_funding
        FROM
            preprocessed pre
        WHERE
            pre.AccountFunding IS NULL
    ''')
    print("dfmiss coubnt ",dfmiss.count())
    dfmiss.show(30,truncate=0)
    
    test_case_count = str(dfmiss.count())
    passed_recs=int(total_recs)-int(test_case_count)
    print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
    if (int(test_case_count)>0):
        teststatus = "Fail"
    else:
        teststatus = "Pass"
    txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
    fwf_data.append(txt)
    
    if dfmiss.count()>0:
        error_data.extend(
        spark.sql(f'''
            SELECT 
            CONCAT(
                {test_case_number}, '|', 
                COALESCE(pre.PayeeID, ''), '|', 
                COALESCE(pre.PaymentID, ''), '|', 
                COALESCE(pre.TransactionCode, ''), '|', 
                COALESCE(pre.AccountFunding, '') 
            ) AS Res 
        FROM 
            preprocessed pre 
        WHERE pre.TransactionCode IS NULL
        ''').rdd.flatMap(lambda x: x).collect())




# print("source join data")

# df_src_1=spark.sql(f'''
#     select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding,p.AccountFunding
#  from payment p join preprocessed pre on p.payeeid = pre.payeeid and p.paymentid = pre.paymentid and p.TransactionCode=pre.TransactionCode
#  where
#     trim(p.TransactionType) = 'A' and
#     trim(p.TransactionCategory){recur_check}RECUR' and
#     substr(trim(p.TransactionCode),10,1) = 'X' and
#     substr(trim(p.TransactionCode), -1) in ('F', 'G', 'H') and
#     split_part(pre.AccountFunding,".", 3) = 'R53'
# ''')
# df_src_1.show(30,truncate=0)
# print("F2........End")
# print("count from source ="+str(df_src_1.count()))


run="weekly"
if run=="monthly":
    jsondata=trans_detail_rec_m
    recur_check='='
    print("Monthly file run")
else:
    jsondata=trans_detail_rec_w
    recur_check='!='
    print("Weekly file run")


for case in jsondata:


    if case=='Regular_Payment':

        test_case_number='1'
        testcase='Regular_Payment'
        print("Regular_Payment source to target data validation")


        df_src_1=spark.sql(f'''
            select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding,p.AccountFunding
        from payment p join preprocessed pre on p.payeeid = pre.payeeid and p.paymentid = pre.paymentid and p.TransactionCode=pre.TransactionCode
        where
            trim(p.TransactionType) = 'A' and
            trim(p.TransactionCategory){recur_check}'RECUR' and
            substr(trim(p.TransactionCode),7,1) = 'Z'and
            substr(trim(p.TransactionCode),10,1) = 'X' and
            substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and
            
            split_part(pre.AccountFunding,".", 3) = '{jsondata[case]['TTC']}'
        ''')

        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Tax_Free":
        test_case_number='2'
        testcase='Tax_Free'
        print("Tax_Free source to target data validation")

        df_src_1=spark.sql(f'''
            select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding as src_funding,p.AccountFunding as tgt_funding
        from payment p join preprocessed pre on p.payeeid = pre.payeeid and p.paymentid = pre.paymentid and p.TransactionCode=pre.TransactionCode
        where
            trim(p.TransactionType) = 'A' and
            trim(p.TransactionCategory){recur_check}'RECUR' and
            substr(trim(p.TransactionCode),10,1) = 'X' and
            substr(trim(p.TransactionCode), -1) in ('F', 'G', 'H') and
            
            split_part(pre.AccountFunding,".", 3) = '{jsondata[case]['TTC']}'
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Convenience":
        test_case_number='3'
        testcase='Convenience'
        print("Convenience source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid 
            AND p.paymentid = pre.paymentid 
            AND p.TransactionCode = pre.TransactionCode
            WHERE 
                TRIM(p.TransactionType) = 'D'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), 18, 1) <> 'H'
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Reimbursement":
        test_case_number='4'
        testcase='Reimbursement'
        print("Reimbursement source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'R'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'X'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass" 

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)
        # fwf_data.append(return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus))

    if case=="Special":
        test_case_number='5'
        testcase='Special'
        print("Special source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'S'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'X'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Novated_Regular_Payment":
        test_case_number='6'
        testcase='Novated_Regular_Payment'
        print("Novated_Regular_Payment source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'Z'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
            
        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Novated_Tax_Free":
        test_case_number='7'
        testcase='Novated_Tax_Free'
        print("Novated_Tax_Free source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) IN ('F', 'G', 'H')
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Novated_Convenience":
        test_case_number='8'
        testcase='Novated_Convenience'
        print("Novated_Convenience source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'D'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) IN ('X', '0')
                AND SUBSTR(TRIM(p.TransactionCode), 18, 1) <> 'H'
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Novated_Reimbursement":
        test_case_number='9'
        testcase='Novated_Reimbursement'
        print("Novated_Reimbursement source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'R'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
            
        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="Novated_Special":
        test_case_number='10'
        testcase='Novated_Special'
        print("Novated_Special source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) IN ('W', 'S')
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)

    if case=="H_Deduction":
        test_case_number='11'
        testcase='H_Deduction'
        print("H_Deduction source to target data validation")
        df_src_1=spark.sql(f'''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid 
            AND p.paymentid = pre.paymentid 
            AND p.TransactionCode = pre.TransactionCode
            WHERE 
                TRIM(p.TransactionType) IN ('D', 'N')
                AND TRIM(p.TransactionCategory){recur_check} 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 18, 1) = 'H'
                
                AND SPLIT_PART(pre.AccountFunding, '.', 3) = '{jsondata[case]['TTC']}';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,testcase)
        fwf_data.append(txt)



# print("fwf_data",fwf_data)
# print("err_data",error_data) 

try:
    
    print("saving......")        
    
    s3Client=boto3.client('s3')


    print("save details records")
    
    s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'Preprocessor_Detail_Record_TestResult_{date_file}.TXT')
    
    print("save details records end")
    
    if len(error_data) > 0:
        
        print("details records errors start here")
        print(error_data)
        s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'Preprocessor_Detail_Record_TestResult_Failures_{date_file}.TXT')
    
        print("details records errors upload end here")
        
except Exception as e:
    print("Excelption Error:", e)




job.commit()
