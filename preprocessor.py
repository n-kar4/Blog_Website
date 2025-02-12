import sys
import time
import re
from pyspark.context import SparkContext
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Processing") \
    .getOrCreate()

# Initialize fwf_data and error_data lists
fwf_data = []
error_data = []
header_txt = str("{:<20}".format("Testcase_number")+"|"+"{:<20}".format("Total_Records_Count")+"|"+"{:<40}".format("Column")+"|"+"{:<10}".format("Pos")+"|"+"{:<30}".format("Default_values")+"|"+"{:<20}".format("Passed_record_count")+"|"+"{:<20}".format("Failed_record_count")+"|"+"{:<10}".format("Test_Status") +"|"+"Test_Case")
fwf_data.append(header_txt)

error_recs = "Testcase_number|PayeeID|PaymentID|TransactionCode|AccountFunding|AccountFunding|Value_in_File|Field"
error_data.append(error_recs)

# Read payment file
payment_df = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='\t', quote='"', escape='"', skip_blank_lines=True).load(f"{'./files/payment.csv'}")
payment_df.createOrReplaceTempView("payment")

# Read p_payment file
p_payment_df = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='\t', quote='"', escape='"', skip_blank_lines=True).load(f"{'./files/p_payment.csv'}")
p_payment_df.createOrReplaceTempView("preprocessed")

transaction_detail_rec = { 
    "Regular_Payment": { "testcasenumber": 1, "TTC": "R01", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Tax_Free": { "testcasenumber": 2, "TTC": "R03", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H" }, 
    "Convenience": { "testcasenumber": 3, "TTC": "R04", "Transaction Type": "D", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "N", "18th character in TransactionCode": "<>H", "Last character in TransactionCode (17th char)": "" }, 
    "Reimbursement": { "testcasenumber": 4, "TTC": "R05", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "R", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Special": { "testcasenumber": 5, "TTC": "R06", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "S", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Novated_Regular_Payment": { "testcasenumber": 6, "TTC": "E01", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Novated_Tax_Free": { "testcasenumber": 7, "TTC": "E03", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H" }, 
#     "TTC": "R05",
#     "Transaction Type": "A",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "R",
#     "10th character in TransactionCode": "X",
#     "18th character in TransactionCode": "",
#     "Last character in TransactionCode (17th char)": "<> F,G or H"
#   },
#   {
#     "Description": "Special",
#     "TTC": "R06",
#     "Transaction Type": "A",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "S",
#     "10th character in TransactionCode": "X",
#     "18th character in TransactionCode": "",
#     "Last character in TransactionCode (17th char)": "<> F,G or H"
#   },
#   {
#     "Description": "Novated_Regular_Payment",
#     "TTC": "E01",
#     "Transaction Type": "A",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "Z",
#     "10th character in TransactionCode": "N",
#     "18th character in TransactionCode": "",
#     "Last character in TransactionCode (17th char)": "<> F,G or H"
#   },
#   {
#     "Description": "Novated_Tax_Free",
#     "TTC": "E03",
#     "Transaction Type": "A",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "n/a",
#     "10th character in TransactionCode": "N",
#     "18th character in TransactionCode": "",
#     "Last character in TransactionCode (17th char)": "= F,G or H"
#   },
#   {
#     "Description": "Novated_Convenience",
#     "TTC": "E04",
#     "Transaction Type": "D",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "n/a",
#     "10th character in TransactionCode": "X or 0",
#     "18th character in TransactionCode": "<>H",
#     "Last character in TransactionCode (17th char)": "n/a"
#   },
#   {
#     "Description": "Novated_Reimbursement",
#     "TTC": "E05",
#     "Transaction Type": "A",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "R",
#     "10th character in TransactionCode": "N",
#     "18th character in TransactionCode": "",
#     "Last character in TransactionCode (17th char)": "<> F,G or H"
#   },
#   {
#     "Description": "Novated_Special",
#     "TTC": "E06",
#     "Transaction Type": "A",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "W or S",
#     "10th character in TransactionCode": "N",
#     "18th character in TransactionCode": "",
#     "Last character in TransactionCode (17th char)": "<> F,G or H"
#   },
#   {
#     "Description": "H Deduction",
#     "TTC": "MH1",
#     "Transaction Type": "D or N",
#     "Transaction Category": "RECUR",
#     "7th character in TransactionCode": "n/a",
#     "10th character in TransactionCode": "n/a",
#     "18th character in TransactionCode": "H",
#     "Last character in TransactionCode (17th char)": "n/a"
#   }
# ]
}

# read payment file
# payment_df = spark.read.csv('./files/payment.csv', header=True,sep='\t')
payment_df = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='\t', quote='"', escape='"', skip_blank_lines=True).load(f"{'./files/payment.csv'}")
payment_df.createOrReplaceTempView("payment")
# payment_df.show(truncate=False)

# read p_payment file
p_payment_df = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='\t', quote='"', escape='"', skip_blank_lines=True).load(f"{'./files/p_payment.csv'}")
p_payment_df.createOrReplaceTempView("preprocessed")

# p_payment_df.show(truncate=False)

transaction_detail_rec = { 
    "Regular_Payment": { "testcasenumber": 1, "TTC": "R01", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Tax_Free": { "testcasenumber": 2, "TTC": "R03", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H" }, 
    "Convenience": { "testcasenumber": 3, "TTC": "R04", "Transaction Type": "D", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "N", "18th character in TransactionCode": "<>H", "Last character in TransactionCode (17th char)": "" }, 
    "Reimbursement": { "testcasenumber": 4, "TTC": "R05", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "R", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Special": { "testcasenumber": 5, "TTC": "R06", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "S", "10th character in TransactionCode": "X", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Novated_Regular_Payment": { "testcasenumber": 6, "TTC": "E01", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "Z", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Novated_Tax_Free": { "testcasenumber": 7, "TTC": "E03", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "= F,G or H" }, 
    "Novated_Convenience": { "testcasenumber": 8, "TTC": "E04", "Transaction Type": "D", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "X or 0", "18th character in TransactionCode": "<>H", "Last character in TransactionCode (17th char)": "" }, 
    "Novated_Reimbursement": { "testcasenumber": 9, "TTC": "E05", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "R", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "Novated_Special": { "testcasenumber": 10, "TTC": "E06", "Transaction Type": "A", "Transaction Category": "RECUR", "7th character in TransactionCode": "W or S", "10th character in TransactionCode": "N", "18th character in TransactionCode": "", "Last character in TransactionCode (17th char)": "<> F,G or H" }, 
    "H_Deduction": { "testcasenumber": 11, "TTC": "MH1", "Transaction Type": "D or N", "Transaction Category": "RECUR", "7th character in TransactionCode": "", "10th character in TransactionCode": "", "18th character in TransactionCode": "H", "Last character in TransactionCode (17th char)": "" } 
}

total_recs = p_payment_df.count()

fwf_data=[]
error_data=[]
header_txt = str("{:<20}".format("Testcase_number")+"|"+"{:<20}".format("Total_Records_Count")+"|"+"{:<40}".format("Column")+"|"+"{:<10}".format("Pos")+"|"+"{:<30}".format("Default_values")+"|"+"{:<20}".format("Passed_record_count")+"|"+"{:<20}".format("Failed_record_count")+"|"+"{:<10}".format("Test_Status") +"|"+"Test_Case")
fwf_data.append(header_txt)

error_recs = "Testcase_number|PayeeID|PaymentID|TransactionCode|AccountFunding|AccountFunding|Value_in_File|Field"


print("source join data")


# print("Regular payment")
# df_src_1=spark.sql('''
#     select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding,p.AccountFunding
#  from payment p join preprocessed pre on p.payeeid = pre.payeeid and p.paymentid = pre.paymentid and p.TransactionCode=pre.TransactionCode
#  where
#     trim(p.TransactionType) = 'A' and
#     trim(p.TransactionCategory)='RECUR' and
#     substr(trim(p.TransactionCode),10,1) = 'X' and
#     substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and
#     split_part(pre.AccountFunding,".", 3) != 'R01'
# ''')

# Define the return_testcase_res function
def return_testcase_res(test_case_number, totalrecs, column, pos, default_values, passed_recs, failed_recs, teststatus, test_case):
    return str("{:<20}".format(test_case_number) + "|" + "{:<20}".format(totalrecs) + "|" + "{:<40}".format(column) + "|" + "{:<10}".format(pos) + "|" + "{:<30}".format(default_values) + "|" + "{:<20}".format(passed_recs) + "|" + "{:<20}".format(failed_recs) + "|" + "{:<10}".format(teststatus) + "|" + test_case)

# get date from json to sreate query


jsondata=transaction_detail_rec

for case in jsondata:
    # query = ""
    # for key, value in jsondata[case].items():
    #     if key == 'TransactionCode' and  value != '':
    #         query += f" and trim(p.TransactionCode)='{value}' "
            
    #     elif key == 'AccountFunding' and  value != '':
    #         query += f" and p.AccountFunding='{value}' "

    #     elif key == '7th character in TransactionCode' and  value != '':
    #         query += f" and substr(trim(p.TransactionCode),7,1)='{value}' "

    #     elif key == '10th character in TransactionCode' and  value != '':
    #         query += f" and substr(trim(p.TransactionCode),10,1)='{value}' "

    #     elif key == '18th character in TransactionCode' and  value != '':
    #         if value[0]=='<>':
    #             query += f" and substr(trim(p.TransactionCode),18,1) not in ('{value[-1]}') "
    #         else:
    #             query += f" and substr(trim(p.TransactionCode),18,1)='{value[-1]}' "

    #     elif key == 'Last character in TransactionCode (17th char)' and  value != '':
    #         if value[0]=='<>':
    #             query += f" and substr(trim(p.TransactionCode),-1) not in ('{value[1]}') "
    #         else:
    #             query += f" and substr(trim(p.TransactionCode),-1)='{value}' "

    #     elif key == 'Transaction Type' and  value != '':
    #         query += f" and trim(p.TransactionType)='{value}' "

    #     elif key == 'Transaction Category' and  value != '':
    #         query += f" and trim(p.TransactionCategory)='{value}' "
    # print(query)


    if case=='Regular_Payment':

        test_case_number='1'
        testcase='Regular_Payment'
        print("Regular_Payment source to target data validation")


        df_src_1=spark.sql('''
            select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding,p.AccountFunding
        from payment p join preprocessed pre on p.payeeid = pre.payeeid and p.paymentid = pre.paymentid and p.TransactionCode=pre.TransactionCode
        where
            trim(p.TransactionType) = 'A' and
            trim(p.TransactionCategory)='RECUR' and
            substr(trim(p.TransactionCode),10,1) = 'X' and
            substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and
            split_part(pre.AccountFunding,".", 3) = 'R01'
        ''')

        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Tax_Free":
        test_case_number='2'
        testcase='Tax_Free'
        print("Tax_Free source to target data validation")

        df_src_1=spark.sql('''
            select pre.PaymentID,pre.TransactionCategory,pre.TransactionType,pre.TransactionCode,pre.AccountFunding as src_funding,p.AccountFunding as tgt_funding
        from payment p join preprocessed pre on p.payeeid = pre.payeeid and p.paymentid = pre.paymentid and p.TransactionCode=pre.TransactionCode
        where
            trim(p.TransactionType) = 'A' and
            trim(p.TransactionCategory)='RECUR' and
            substr(trim(p.TransactionCode),7,1) = 'Z' and
            substr(trim(p.TransactionCode),10,1) = 'X' and
            substr(trim(p.TransactionCode), -1) not in ('F', 'G', 'H') and
            split_part(pre.AccountFunding,".", 3) = 'R01'
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Convenience":
        test_case_number='3'
        testcase='Convenience'
        print("Convenience source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid 
            AND p.paymentid = pre.paymentid 
            AND p.TransactionCode = pre.TransactionCode
            WHERE 
                TRIM(p.TransactionType) = 'D'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), 18, 1) <> 'H'
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'R04';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Reimbursement":
        test_case_number='4'
        testcase='Reimbursement'
        print("Reimbursement source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'R'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'X'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'R05';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass" 

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)
        # fwf_data.append(return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus))

    if case=="Special":
        test_case_number='5'
        testcase='Special'
        print("Special source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'S'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'X'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'R06';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Novated_Regular_Payment":
        test_case_number='6'
        testcase='Novated_Regular_Payment'
        print("Novated_Regular_Payment source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'Z'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'E01';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
            
        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Novated_Tax_Free":
        test_case_number='7'
        testcase='Novated_Tax_Free'
        print("Novated_Tax_Free source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) IN ('F', 'G', 'H')
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'E03';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Novated_Convenience":
        test_case_number='8'
        testcase='Novated_Convenience'
        print("Novated_Convenience source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'D'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) IN ('X', '0')
                AND SUBSTR(TRIM(p.TransactionCode), 18, 1) <> 'H'
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'E04';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Novated_Reimbursement":
        test_case_number='9'
        testcase='Novated_Reimbursement'
        print("Novated_Reimbursement source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) = 'R'
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'E05';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
            
        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="Novated_Special":
        test_case_number='10'
        testcase='Novated_Special'
        print("Novated_Special source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid
            AND p.paymentid = pre.paymentid
            AND p.TransactionCode = pre.TransactionCode
            WHERE
                TRIM(p.TransactionType) = 'A'
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 7, 1) IN ('W', 'S')
                AND SUBSTR(TRIM(p.TransactionCode), 10, 1) = 'N'
                AND SUBSTR(TRIM(p.TransactionCode), -1) NOT IN ('F', 'G', 'H')
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'E06';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)

    if case=="H_Deduction":
        test_case_number='11'
        testcase='H_Deduction'
        print("H_Deduction source to target data validation")
        df_src_1=spark.sql('''
            SELECT pre.PaymentID, pre.TransactionCategory, pre.TransactionType, pre.TransactionCode, pre.AccountFunding, p.AccountFunding
            FROM payment p
            JOIN preprocessed pre
            ON p.payeeid = pre.payeeid 
            AND p.paymentid = pre.paymentid 
            AND p.TransactionCode = pre.TransactionCode
            WHERE 
                TRIM(p.TransactionType) IN ('D', 'N')
                AND TRIM(p.TransactionCategory) = 'RECUR'
                AND SUBSTR(TRIM(p.TransactionCode), 18, 1) = 'H'
                AND SPLIT_PART(pre.AccountFunding, '.', 3) != 'MH1';
        ''')
        df_src_1.show(30,truncate=0)
        test_case_count = str(df_src_1.count())
        passed_recs=int(total_recs)-int(test_case_count)
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"

        txt = return_testcase_res(test_case_number,total_recs,'AccountFunding','85-100','NA',str(passed_recs),str(test_case_count),teststatus,teststatus)
        fwf_data.append(txt)



print("fwf_data",fwf_data)
print("err_data",error_data) 

# create a text file writing fwf_data

with open('fwf_data.txt', 'w') as f:
    for item in fwf_data:
        f.write("%s\n" % item)

# create a text file writing error_data





        








# df_src_1.show(30,truncate=0)
# print("F2........End")
# print("count from source ="+str(df_src_1.count()))