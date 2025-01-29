test_erf1_uat1
------------------------

import sys
import time
import boto3
import csv
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3 = boto3.resource('s3')

cur_date=str(time.strftime('%Y%m%d'))
day_of_year=datetime.now().timetuple().tm_yday
year=str(time.strftime('%y'))
date_file=str(time.strftime('%Y%m%d%H%M%S'))


tgt_bucket='uat1-gwf-cc-cats-filexfer-us-east-1'
tgt_key='outbound/CaTS_Automation_Testing/erf1/test_results/'

#Source files
# src_s3uri = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/inbound/phoenix/XF0000TR_20241029*.FIL"
src_s3uri = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/archive/2025-01-21/ERF1_PHX_TO_ERF/Daily/Run_no6/XF0000TR.FIL"


try:
    # Target
    # s3_obj = s3.Object(bucket_name = 'uat1-gwf-cc-cats-filexfer-us-east-1', key = 'outbound/erf/ERF_DB_TRANS')
    s3_obj = s3.Object(bucket_name = 'uat1-gwf-cc-cats-filexfer-us-east-1', key = 'archive/2025-01-21/ERF1_PHX_TO_ERF/Daily/Run_no6/ERF_DB_TRANS')
    
    #s3uri = "s3://uat2-gwf-cc-cats-publish-class0-us-east-1/release01/2024-08-26/erf1/ERF_DB_TRANS"
    s3 = s3_obj.get()
    s3_obj_body = s3.get('Body')
    detail = s3_obj_body.read().decode()
    #print(type(detail))
    print(detail)
except s3.meta.client.exceptions.NoSuchBucket as e:
    print("No such Bucket")
    print(e)
    
except s3.meta.client.exceptions.NoSuchKey as e:
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
    print("Target records count "+str(detail_df.shape[0]))
    
    detail_df.to_csv("s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/tgt_ERF1_File.txt", header = False, index =False, encoding= 'utf-8')
    print("Target split line file created")
    
    trailer_df.to_csv("s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/trailer_ERF1_File.txt", header = False, index =False, encoding= 'utf-8')
    print("Trailer split line file created")
    
    
    #Fetch Total amount, record count from trailer record
    triler_rec = str(trailer_df.iloc[0].to_string(header = False, index =False))
    print("Trailer_Rec:"+triler_rec)
    target_rec_count= triler_rec[2:8]
    target_tot_amount= triler_rec[24:43]
    print("trailer record count:"+str(target_rec_count)+"::trailer total amount:"+target_tot_amount)
    
except Exception as e:
    print("Exception Error:", e)
    


# ######################################################################################

erf1_detail_rec =  {
"PARIS_REC_TYPE":{"testcasenumber":1,"pos":"1-2","len":2,"default":"no","default_val":"NA","testcase":"default value check"}, 
"CNTR_NUM":{"testcasenumber":2,"pos":"3-8","len":6,"default":"no","default_val":"NA","testcase":"Null check","req":"yes"}, 
"CNTR_TYPE":{"testcasenumber":3,"pos":"9-11","len":3,"default":"no","default_val":"NA","testcase":"Null check check","req":"yes"}, 
"FUND_ID":{"testcasenumber":4,"pos":"12-14","len":3,"default":"no","default_val":"NA","testcase":"Null check check","req":"yes"}, 
"CNTR_SUB":{"testcasenumber":5,"pos":"15-17","len":3,"default":"no","default_val":"NA","testcase":"Null check check","req":"yes"}, 
"PREM_TAX":{"testcasenumber":6,"pos":"18-18","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"TB":{"testcasenumber":7,"pos":"19-20","len":2,"default":"yes","default_val":"s_2","testcase":"default value check"}, 
"TCN":{"testcasenumber":8,"pos":"21-30","len":10,"default":"yes","default_val":"s_10","testcase":"default value check"}, 
"ORIG_TTC":{"testcasenumber":9,"pos":"31-35","len":5,"default":"no","default_val":"NA","testcase":"default value check"}, 
"SPEC_TTC":{"testcasenumber":10,"pos":"36-40","len":5,"default":"no","default_val":"s_5","testcase":"default value check"}, 
"FUND_EFF":{"testcasenumber":11,"pos":"41-41","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"ASSET_EFF":{"testcasenumber":12,"pos":"42-42","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"CNTR_DT":{"testcasenumber":13,"pos":"43-52","len":10,"default":"no","default_val":"NA","testcase":"default value check"}, 
"VCH_DT":{"testcasenumber":14,"pos":"53-62","len":10,"default":"no","default_val":"NA","testcase":"default value check"}, 
"SOURCE_SYSTEM":{"testcasenumber":15,"pos":"63-68","len":6,"default":"yes","default_val":"s_6","testcase":"default value check"}, 
"GEN_YEAR":{"testcasenumber":16,"pos":"69-72","len":4,"default":"yes","default_val":"s_4","testcase":"default value check"}, 
"GEN_PERIOD":{"testcasenumber":17,"pos":"73-73","len":1,"default":"yes","default_val":"s_1","testcase":"default value check"}, 
"BUS_TYPE":{"testcasenumber":18,"pos":"74-79","len":6,"default":"yes","default_val":"s_6","testcase":"default value check"}, 
"SRVC_AGREE":{"testcasenumber":19,"pos":"80-84","len":5,"default":"yes","default_val":"s_5","testcase":"default value check"}, 
"TRANS_AMOUNT":{"testcasenumber":21,"pos":"85-100","len":16,"default":"no","default_val":"NA","testcase":"default value check"}, 
"LEGACY_CNTR":{"testcasenumber":22,"pos":"101-107","len":7,"default":"yes","default_val":"s_7","testcase":"default value check"}, 
"LEGACY_SUB_FUND":{"testcasenumber":23,"pos":"108-109","len":2,"default":"yes","default_val":"s_2","testcase":"default value check"}, 
"LEGACY_FUND":{"testcasenumber":24,"pos":"110-116","len":7,"default":"yes","default_val":"s_7","testcase":"default value check"}, 
"NESTED_FUND_ID":{"testcasenumber":25,"pos":"117-119","len":3,"default":"yes","default_val":"s_3","testcase":"default value check"}, 
"DTR_TTC":{"testcasenumber":26,"pos":"120-124","len":5,"default":"no","default_val":"NA","testcase":"default value check"}, 
"FILLER":{"testcasenumber":27,"pos":"125-200","len":76,"default":"yes","default_val":"s_76","testcase":"default value check"}
}

erf1_trailer_rec = {
"WS_TRL_REC_TYPE":{"testcasenumber":1,"pos":"1-2","len":2,"default":"yes","default_val":"TR","testcase":"default value check"}, 
"WS_TRL_ROW_COUNT":{"testcasenumber":2,"pos":"3-8","len":6,"default":"no","default_val":"NA","testcase":"default value check"}, 
"WS_TRL_HASH_TOTAL":{"testcasenumber":3,"pos":"9-24","len":16,"default":"yes","default_val":"s_16","testcase":"default value check"}, 
"WS_TRL_TOT_TRANS_AMT":{"testcasenumber":4,"pos":"25-43","len":19,"default":"no","default_val":"NA","testcase":"default value check"}, 
"FILLER":{"testcasenumber":5,"pos":"44-200","len":157,"default":"yes","default_val":"s_157","testcase":"default value check"}
}

#Target
s3uri = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/tgt_ERF1_File.txt"



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
 
rectype ='h'
rec_val = 'TR'

if rectype == 'h':   
    logFormat = generate_custom_grok_pattern(erf1_header_rec)
    jsondata = erf1_header_rec
    rec_val = ' '
if rectype == 'd':
    logFormat = generate_custom_grok_pattern(erf1_detail_rec)
    jsondata = erf1_detail_rec
    rec_val = '01'
if rectype == 't':
    logFormat = generate_custom_grok_pattern(erf1_trailer_rec)
    jsondata = erf1_trailer_rec
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
tgtDF = grokglueDynamicFrame.toDF().repartition(1)
detailed_rec_cnt = tgtDF.count()
print(tgtDF.printSchema())
#detailed_rec_cnt = tgtDF1.filter(tgtDF1.PARIS_REC_TYPE == rec_val).count()
#tgtDF = tgtDF1.filter(tgtDF1.PARIS_REC_TYPE == rec_val)
tgtDF.cache()
tgtDF.show(30)

tgtDF.createOrReplaceTempView("tgtdata")
totalrecs=str(spark.sql("select count(1) from tgtdata").collect()[0][0])
print("Total target records....."+str(totalrecs))
    

try:
    #---------------------------------------------------------------
    # Source
    
    print("Source data")
    
    
    erf1_src =  {
    "Fund_number":{"pos":"1-10","len":10}, 
    "Shareholder_Account_Number":{"pos":"11-19","len":9},
    "Dummy1":{"pos":"20-32","len":13},
    "Date_Of_Trade":{"pos":"33-40","len":8},
    "Dummy2":{"pos":"41-45","len":5},
    "Transaction_Type":{"pos":"46-48","len":3},
    "Dummy3":{"pos":"49-81","len":33},
    "Reversal_ind":{"pos":"82-82","len":1},
    "Dummy4":{"pos":"83-100","len":18},
    "Dollar_Amount":{"pos":"101-119","len":19},
    "Dummy5":{"pos":"120-218","len":99},
    "Process_Date":{"pos":"219-226","len":8}
    }
    
    logFormat = generate_custom_grok_pattern(erf1_src)


    print("Read all transaction history files")
    transaction_df_src = spark.read.format('com.databricks.spark.csv').options(header=False, delimiter='|', quote='"', escape=',', skip_blank_lines=True).load(f"{src_s3uri}")
    print(transaction_df_src.printSchema())
    transaction_df_temp = transaction_df_src.withColumn('new',regexp_replace(transaction_df_src[0],',',' '))
    transaction_df_temp.show(truncate=0)
    transaction_df = transaction_df_temp.select("new")
    transaction_df.createOrReplaceTempView("trns")
    print(transaction_df.printSchema())
    spark.sql("select * from trns limit 20").show(truncate=0)
    
    # quoting = csv.QUOTE_NONE,
    pd_df=transaction_df.toPandas()
    pd_df.to_csv("s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/src_transactions/Src_ERF1_trns.txt", header = False, index =False, encoding='utf-8', escapechar='\\', quoting = csv.QUOTE_NONE)
    
    print("Append columns to transaction history files")

    src_s3uri = 's3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/src_transactions/Src_ERF1_trns.txt'
    
    src_grokglueDynamicFrame = read_grok_parsed_dynamic_frame(glueContext, src_s3uri, logFormat)

    src_grokglueDynamicFrame.show()        
    phoenix_transaction_history_df_src = src_grokglueDynamicFrame.toDF().repartition(1)
    phoenix_transaction_history_df = phoenix_transaction_history_df_src.filter(phoenix_transaction_history_df_src.Fund_number != 'XF0000TR.F')
    print(phoenix_transaction_history_df.printSchema())
    phoenix_transaction_history_df.createOrReplaceTempView("phoenix_transaction_history")
    print(phoenix_transaction_history_df.printSchema())
    phoenix_transaction_history_df.cache()
    phoenix_transaction_history_df.show(30)


    # ######################################################################################
    # erf_contracts_w_Fund_ID_with_GG_Lookup
    
    print("Source data erf_contracts_w_Fund_ID_with_GG_Lookup")
    # erf_contracts_w_Fund_ID_with_GG_Lookup_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/erf_contracts_w_Fund_ID_with_GG_Lookup.csv"
    erf_contracts_w_Fund_ID_with_GG_Lookup_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/archive/2025-01-21/ERF1_PHX_TO_ERF/Daily/Run_no6/erf_contracts_w_Fund_ID_with_GG_Lookup.csv"
    erf_contracts_w_Fund_ID_with_GG_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{erf_contracts_w_Fund_ID_with_GG_Lookup_path}")
    erf_contracts_w_Fund_ID_with_GG_DF.createOrReplaceTempView("erf1_erf_contracts_with_fund_id_gg_lookup")
    print(erf_contracts_w_Fund_ID_with_GG_DF.printSchema())
    spark.sql("select * from erf1_erf_contracts_with_fund_id_gg_lookup limit 30").show(truncate=0)
    
    
    # ######################################################################################
    # ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds
    
    print("Source data ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds")
    # ttc_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds.csv"
    ttc_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/archive/2025-01-21/ERF1_PHX_TO_ERF/Daily/Run_no6/ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds.csv"
    ttc_path_df = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter=',', quote='"', escape='"', skip_blank_lines=True).load(f"{ttc_path}")
    ttc_path_df.createOrReplaceTempView("erf1_erf_cat_code_and_cat_item_code_4_paris_ttc_w_mapping_2_empower_cds")
    print(ttc_path_df.printSchema())
    spark.sql("select * from erf1_erf_cat_code_and_cat_item_code_4_paris_ttc_w_mapping_2_empower_cds limit 30").show(truncate=0)
    
    # ######################################################################################
    #Source Query
    
    print("Source data .......")
    
    src_df=spark.sql('''
    SELECT 
    src.shareholder_account_number,src.fund_number,src.Transaction_Type,
    case when trim(tcc.Phoenix_Description) = 'Variable Fee Paid' then '02'
    else '01' end as PARIS_REC_TYPE, 
    gg.CNTRCT_NUM as CNTR_NUM,
    gg.CNTRCT_TYP_CD as CNTR_TYPE, 
    gg.fund_id,
    gg.PRU_Contract_SUB as CNTR_SUB,
    tcc.Trans_Typ_Cd as ORIG_TTC,
    tcc.Phoenix_Description,
    gg.cntrct_num,
    gg.cntrct_typ_cd,
    tcc.Trans_Typ_Cd as DTR_TTC,
    src.date_of_trade as CNTR_DT, 
    src.process_date, 
    dollar_amount,
    case when Reversal_ind == '1' and cast(trim(dollar_amount) as varchar(30)) not like  '-%'
            then concat('-',lpad(cast(cast(cast(trim(dollar_amount) as varchar(30)) as decimal(20,2)) as varchar(30)),15,'0'))
        when Reversal_ind == '1' and cast(trim(dollar_amount) as varchar(30)) like  '-%'
            then concat('+',lpad(cast(cast(replace(cast(trim(dollar_amount) as varchar(30)),'-','') as decimal(20,2)) as varchar(30)),15,'0'))
        when cast(trim(dollar_amount) as varchar(30)) like  '-%'
            then concat('-',lpad(cast(cast(replace(cast(trim(dollar_amount) as varchar(30)),'-','') as decimal(20,2)) as varchar(30)),15,'0'))
                    else concat('+',lpad(cast(cast(replace(cast(trim(dollar_amount) as varchar(30)),'-','') as decimal(20,2)) as varchar(30)),15,'0')) end as TRANS_AMOUNT
    FROM phoenix_transaction_history  src
    inner join erf1_erf_contracts_with_fund_id_gg_lookup gg
    on trim(src.shareholder_account_number) = trim(gg.easy_group_account_id)
    and trim(src.fund_number) = trim(gg.sdio_id)
    inner join  erf1_erf_cat_code_and_cat_item_code_4_paris_ttc_w_mapping_2_empower_cds tcc
    on trim(src.Transaction_Type) = trim(tcc.Phoenix_Code)
    where dollar_amount <> '0.00' ''')
    
    print("count from source ="+str(src_df.count()))
    
    src_df.createOrReplaceTempView("srcdata")
    src_df.show(40,truncate=0)
    
    # Save Source data
    # src_pd_df=src_df.toPandas()
    # src_pd_df.to_csv("s3://uat2-gwf-cc-cats-filexfer-us-east-1/outbound/phoenix_testing/cats/erf1/src_data/src_data.txt", header = True, index =False, encoding='utf-8', escapechar='\\', quoting = csv.QUOTE_NONE)
    
    Save_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/src_data/"
    src_df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).mode("overwrite").save(f"{Save_PATH}")

    
    #---------------------------------------------------------------
    # Total amount validations: source checks
    # src_total_amt=str(spark.sql("select case when cast(trim(sum(dollar_amount)) as varchar(40)) like  '-%' then concat('-',lpad(cast(cast(replace(cast(trim(sum(dollar_amount)) as varchar(40)),'-','') as decimal(25,2)) as varchar(40)),18,'0')) else concat('+',lpad(cast(cast(replace(cast(trim(sum(dollar_amount)) as varchar(40)),'-','') as decimal(25,2)) as varchar(40)),18,'0')) end as Total_AMOUNT from srcdata").collect()[0][0])
    
    
    src_total_amt=str(spark.sql("select case when cast(trim(sum(TRANS_AMOUNT)) as varchar(40)) like  '-%' then concat('-',lpad(cast(cast(replace(cast(trim(sum(TRANS_AMOUNT)) as varchar(40)),'-','') as decimal(25,2)) as varchar(40)),18,'0')) else concat('+',lpad(cast(cast(replace(cast(trim(sum(TRANS_AMOUNT)) as varchar(40)),'-','') as decimal(25,2)) as varchar(40)),18,'0')) end as Total_AMOUNT from srcdata").collect()[0][0])
    
                    
    
    fwf_data=[]
    error_data=[]
    header_txt = str("{:<20}".format("Testcase_number")+"|"+"{:<20}".format("Total_Records_Count")+"|"+"{:<40}".format("Column")+"|"+"{:<10}".format("Pos")+"|"+"{:<30}".format("Default_values")+"|"+"{:<20}".format("Passed_record_count")+"|"+"{:<20}".format("Failed_record_count")+"|"+"{:<10}".format("Test_Status") +"|"+"Test_Case")
    fwf_data.append(header_txt)
    
    error_recs = "Testcase_number|CNTR_NUM|FUND_ID|Value_in_File|Field|Test_Case_Description"
    
    error_data.append(error_recs)
    
    
    def return_testcase_res(test_case_number,totalrecs,columnname,pos,defaultvalues,passedrecs,failedrecs,teststatus,testcase):
        return str("{:<20}".format(test_case_number)+"|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(columnname)+"|"+"{:<10}".format(pos)+"|"+"{:<30}".format(str(defaultvalues))+"|"+"{:<20}".format(str(passedrecs))+"|"+"{:<20}".format(failedrecs)+"|"+"{:<10}".format(teststatus)+"|"+"{:<200}".format(testcase))
    
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
                        error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',FUND_ID,'|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")+" limit 100").rdd.flatMap(lambda x:x).collect())
                    
                    
                txt = str("{:<20}".format(test_case_number)+ "|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(c)+"|"+"{:<10}".format(jsondata[c]['pos'])+"|"+"{:<30}".format(str(jsondata[c]['default_val']))+ "|"+"{:<20}".format(passedrecs)+"|"+"{:<20}".format(failedrecs) +"|"+"{:<10}".format(teststatus) + "|"+"{:<200}".format(test_case))
                fwf_data.append(txt)    
        
        
        
        # check source to target missing CNTR_NUM, PARIS_REC_TYPE
        if (c =='PARIS_REC_TYPE'):
            
            test_case_number='1'
            test_case_desc = 'check source to target missing CNTR_NUM, PARIS_REC_TYPE'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTR_NUM,s.PARIS_REC_TYPE
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM)
    					and trim(t.PARIS_REC_TYPE) = trim(s.PARIS_REC_TYPE)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(distinct s.CNTR_NUM,s.PARIS_REC_TYPE)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM)
    					and trim(t.PARIS_REC_TYPE) = trim(s.PARIS_REC_TYPE)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',Fund_Id,'|',PARIS_REC_TYPE,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) and trim(t.PARIS_REC_TYPE) = trim(s.PARIS_REC_TYPE)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTR_NUM
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) and trim(t.PARIS_REC_TYPE) = trim(s.PARIS_REC_TYPE))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'1-2','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
        
        # check source to target missing CNTR_NUM
        if (c =='CNTR_NUM'):
            
            test_case_number='2'
            test_case_desc = 'Source to target missing CNTR_NUM'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTR_NUM
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(distinct s.CNTR_NUM)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',Fund_Id,'|',CNTR_NUM,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTR_NUM) = trim(s.CNTR_NUM)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTR_NUM
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'3-8','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
       

        # check source to target missing FUND_ID
        if (c =='FUND_ID'):
            
            test_case_number='4'
            test_case_desc = 'source to target missing FUND_ID'
            print(test_case_desc)
            spark.sql('''select distinct s.FUND_ID
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.FUND_ID) = trim(s.FUND_ID)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(distinct s.Fund_number)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.FUND_ID) = trim(s.FUND_ID)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',FUND_ID,'|',FUND_ID,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.FUND_ID) = trim(s.FUND_ID)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.Fund_number
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.FUND_ID) = trim(s.FUND_ID))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'12-14','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
        
        # source to target missing CNTR_NUM,FUND_ID,CNTR_TYPE
        if (c =='CNTR_TYPE'):
            
            test_case_number='3'
            test_case_desc = 'source to target missing CNTR_NUM,FUND_ID,CNTR_TYPE'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.CNTR_TYPE
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE) 
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(distinct s.CNTR_NUM,s.FUND_ID,s.CNTR_TYPE)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE) 
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',FUND_ID,'|',CNTR_TYPE,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) and trim(t.FUND_ID) = trim(s.FUND_ID) and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.CNTR_TYPE
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'9-11','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
        
        # source to target missing CNTR_NUM,FUND_ID,CNTR_TYPE,TRANS_AMOUNT
        if (c =='TRANS_AMOUNT'):
            
            test_case_number='21'
            test_case_desc = 'source to target missing CNTR_NUM,FUND_ID,CNTR_TYPE,TRANS_AMOUNT'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.CNTR_TYPE
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE) 
    					and trim(t.TRANS_AMOUNT) = trim(s.TRANS_AMOUNT) 
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(distinct s.CNTR_NUM,s.FUND_ID,s.CNTR_TYPE,s.TRANS_AMOUNT)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE) 
    					and trim(t.TRANS_AMOUNT) = trim(s.TRANS_AMOUNT)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',FUND_ID,'|',TRANS_AMOUNT,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) and trim(t.FUND_ID) = trim(s.FUND_ID) and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE) and trim(t.TRANS_AMOUNT) = trim(s.TRANS_AMOUNT)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.CNTR_TYPE,s.TRANS_AMOUNT
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE)
    					and trim(t.TRANS_AMOUNT) = trim(s.TRANS_AMOUNT))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'85-100','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
        
        
        
        # source to target missing CNTR_NUM,FUND_ID,DTR_TTC
        if (c =='DTR_TTC'):
            
            test_case_number='26'
            test_case_desc = 'source to target missing CNTR_NUM,FUND_ID,DTR_TTC'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.DTR_TTC
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.DTR_TTC) = trim(s.DTR_TTC) 
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(distinct s.CNTR_NUM,s.FUND_ID,s.DTR_TTC)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.DTR_TTC) = trim(s.DTR_TTC) 
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',FUND_ID,'|',DTR_TTC,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) and trim(t.FUND_ID) = trim(s.FUND_ID) and trim(t.DTR_TTC) = trim(s.DTR_TTC)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.DTR_TTC
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.DTR_TTC) = trim(s.DTR_TTC))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'120-124','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
        
        
        # source to target data checks on CNTR_DT
        if (c =='CNTR_DT'):
            
            test_case_number='13'
            test_case_desc = 'source to target data checks on CNTR_DT'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.DTR_TTC,s.CNTR_DT
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.DTR_TTC) = trim(s.DTR_TTC)
    					and replace(trim(t.CNTR_DT),'-','') = trim(s.CNTR_DT)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(distinct s.CNTR_NUM,s.FUND_ID,s.DTR_TTC,s.CNTR_DT)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.DTR_TTC) = trim(s.DTR_TTC) 
    					and replace(trim(t.CNTR_DT),'-','') = trim(s.CNTR_DT)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTR_NUM,'|',FUND_ID,'|',CNTR_DT,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) and trim(t.FUND_ID) = trim(s.FUND_ID) and trim(t.DTR_TTC) = trim(s.DTR_TTC) and replace(trim(t.CNTR_DT),'-','') = trim(s.CNTR_DT)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.DTR_TTC,s.CNTR_DT
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.DTR_TTC) = trim(s.DTR_TTC)
    					and replace(trim(t.CNTR_DT),'-','') = trim(s.CNTR_DT))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'120-124','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)
        
        
        # Trailer record: Total amount checks from source to target
        if (c=='TRANS_AMOUNT'):
            
            test_case_number='30'
            test_case_desc = 'Trailer record: Total amount checks from source to target'
            print(test_case_desc)
            print("src_total_amt:"+src_total_amt+"::target_tot_amount:"+target_tot_amount)
            if (target_tot_amount != src_total_amt ):
                teststatus = "Fail"
                test_case_count=1
                passed_recs=0
            else:
                teststatus = "Pass"
                test_case_count=0
                passed_recs=1
            
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|','NA','|','NA','|',"+src_total_amt+",'|','WS-TRL-TOT-TRANS-AMT','|','"+test_case_desc+"') as Res from (select 'X') ").rdd.flatMap(lambda x:x).collect())
                    
                
            txt = return_testcase_res(test_case_number,totalrecs,'WS-TRL-TOT-TRANS-AMT','25-43','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)
        
    print("saving......")        
    
    s3Client=boto3.client('s3')

    if rectype == 'h':   
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF1_Header_Record_TestResult_{date_file}.TXT')    
        if len(error_data) > 0:
            
            print("header records errors start here")
            print(error_data)
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF1_Header_Record_TestResult_Failures_{date_file}.TXT')
        
            print("details records errors end here")
    if rectype == 'd':
        print("save details records")
        
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF1_Detail_Record_TestResult_{date_file}.TXT')
        
        print("save details records end")
        
        if len(error_data) > 0:
            
            print("details records errors start here")
            print(error_data)
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF1_Detail_Record_TestResult_Failures_{date_file}.TXT')
        
            print("details records errors end here")
            
    if rectype == 't':
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF1_Trailer_Record_RestResult_{date_file}.TXT')
        
        if len(error_data) > 0:
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF1_Trailer_Record_TestResult_Failure_Recs_{date_file}.txt')
                  
            
except Exception as e:
    print("Exception Error:", e)


job.commit()





------------------------------------------------------------------------------------------------------


test_uat2_ads_18a
-------------------

import sys
import time
import boto3
import csv
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from datetime import datetime
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3 = boto3.resource('s3')

cur_date=str(time.strftime('%Y%m%d'))
day_of_year=datetime.now().timetuple().tm_yday
year=str(time.strftime('%y'))
date_file=str(time.strftime('%Y%m%d%H%M%S'))

tgt_bucket='uat2-gwf-cc-cats-filexfer-us-east-1'
tgt_key='outbound/phoenix_testing/cats/ads-18a/test_results/'

try:
    
    # Target file path
    
    print("Target file path")
    #tgt_RET_ERF_MFL_ME_path ="s3://uat2-gwf-cc-cats-publish-class0-us-east-1/release01/2024-07-25/RET_ERF_ERU_ME.txt"
    tgt_RET_ERF_MFL_ME_path ="s3://uat2-gwf-cc-cats-filexfer-us-east-1/outbound/ads/RET_ERF_MFL_ME_PROD_07312024.txt" #PROD_validation
    
    # ######################################################################################
    # Source data RET.ERF.MFL.ME
    
    print("Source data RET.ERF.ERU.ME")
    #src_RET_ERF_MFL_ME_path ="s3://uat2-gwf-cc-cats-filexfer-us-east-1/inbound/erf/mfl/RET_ERF_MFL_ME.txt"
    #src_RET_ERF_MFL_ME_path ="s3://uat2-gwf-cc-cats-filexfer-us-east-1/inbound/erf/RET.ERF.MFL.ME.20240415.01"
    src_RET_ERF_ERU_ME_path = "s3://uat2-gwf-cc-cats-filexfer-us-east-1/inbound/erf/RET.ERF.ERU.ME.20240415.01"
    
    
    RET_ERF_ERU_ME_DF_TEMP = spark.read.format('com.databricks.spark.csv').options(header='False', delimiter=',', quote='"', escape='"', skip_blank_lines=True).load(f"{src_RET_ERF_ERU_ME_path}")
    RET_ERF_ERU_ME_DF = (RET_ERF_ERU_ME_DF_TEMP.withColumnRenamed('_c1','CNTRCT_NUM').withColumnRenamed('_c2','CNTRCT_SUBDIV_CD').withColumnRenamed('_c3','INT_RTE'))
    RET_ERF_ERU_ME_DF.createOrReplaceTempView("RET_ERF_ERU_ME")
    print(RET_ERF_ERU_ME_DF.printSchema())
    spark.sql("select * from RET_ERF_ERU_ME limit 30").show(truncate=0)
    
    
    # ######################################################################################
    # Lookup file erf_contracts_w_Fund_ID_with_GG_Lookup
    
    print("Lookup file erf_contracts_w_Fund_ID_with_GG_Lookup")
    #Fund_ID_with_GG_Lookup_path ="s3://uat2-gwf-cc-cats-filexfer-us-east-1/outbound/phoenix_testing/cats/ads-18a/erf_contracts_w_Fund_ID_with_GG_Lookup.csv"
    Fund_ID_with_GG_Lookup_path ="s3://uat2-gwf-cc-cats-filexfer-us-east-1/inbound/lookup/erf_contracts_with_fund_id_gg_lookup/erf_contracts_w_Fund_ID_with_GG_Lookup.csv"
    
    Fund_ID_with_GG_Lookup = spark.read.format('com.databricks.spark.csv').options(header='True', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{Fund_ID_with_GG_Lookup_path}")
    Fund_ID_with_GG_Lookup.createOrReplaceTempView("Fund_ID_with_GG_Lookup")
    print(Fund_ID_with_GG_Lookup.printSchema())
    spark.sql("select * from Fund_ID_with_GG_Lookup limit 30").show(truncate=0)
    
    # ######################################################################################
    # Src Query
    
    print("Source Data")
    src_DF=spark.sql("select src.CNTRCT_NUM,src.CNTRCT_SUBDIV_CD,src.INT_RTE,case when gg.EASY_Group_Client_Id is null then lpad(src.CNTRCT_NUM,10,'0') else lpad(gg.EASY_Group_Client_Id,10,'0') end as EASY_Group_Client_Id,case when gg.EASY_Group_Account_Id is null then src.CNTRCT_NUM else gg.EASY_Group_Account_Id end as EASY_Group_Account_Id,coalesce(gg.SDIO_ID,'') as SDIO_ID from RET_ERF_ERU_ME src left join Fund_ID_with_GG_Lookup gg on src.CNTRCT_NUM =gg.CNTRCT_NUM")
    src_DF.show(truncate=0)
    src_DF.createOrReplaceTempView('srcdata')
    
    print("count from source ="+str(src_DF.count()))
 
    src_Save_PATH ="s3://uat2-gwf-cc-cats-filexfer-us-east-1/outbound/phoenix_testing/cats/ads-18a/src_data/"
    src_DF.coalesce(1).write.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).mode("overwrite").save(f"{src_Save_PATH}")
    
    
    # ######################################################################################
    # Target data
    
    print("Target table")
    #s3://uat2-gwf-cc-cats-filexfer-us-east-1/outbound/ads/RET_ERF_ERU_ME.txt
    tgt_RET_ERF_MFL_ME_temp = spark.read.format('com.databricks.spark.csv').options(header='False', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{tgt_RET_ERF_MFL_ME_path}")
    
    tgt_RET_ERF_MFL_ME = (tgt_RET_ERF_MFL_ME_temp.withColumnRenamed('_c0','CNTRCT_NUM').withColumnRenamed('_c1','CNTRCT_SUBDIV_CD').withColumnRenamed('_c2','INT_RTE').withColumnRenamed('_c3','EASY_Group_Client_Id').withColumnRenamed('_c4','EASY_Group_Account_Id').withColumnRenamed('_c5','SDIO_ID'))
    tgt_RET_ERF_MFL_ME.createOrReplaceTempView("tgtdata")
    print(tgt_RET_ERF_MFL_ME.printSchema())
    spark.sql("select * from tgtdata limit 30").show(truncate=0)
    totalrecs=str(spark.sql("select count(1) from tgtdata").collect()[0][0])
    print("Total target records....."+str(totalrecs))
    
    
    # ######################################################################################

    ads_18a_detail_rec =  {
    "CNTRCT_NUM":{"testcasenumber":1,"pos":"1-6","len":6,"default":"no","default_val":"NA","testcase":"Required field validation","req":"yes"}, 
    "CNTRCT_SUBDIV_CD":{"testcasenumber":2,"pos":"8-10","len":3,"default":"no","default_val":"NA","testcase":"Required field validation","req":"yes"}, 
    "INT_RTE":{"testcasenumber":6,"pos":"36-42","len":7,"default":"no","default_val":"NA","testcase":"Required field validation","req":"yes"}, 
    "EASY_Group_Client_Id":{"testcasenumber":7,"pos":"44-53","len":10,"default":"no","default_val":"NA","testcase":"Required field validation","req":"yes"}, 
    "EASY_Group_Account_Id":{"testcasenumber":8,"pos":"55-67","len":13,"default":"no","default_val":"NA","testcase":"Required field validation","req":"yes"}, 
    "SDIO_ID":{"testcasenumber":9,"pos":"69-74","len":6,"default":"no","default_val":"NA","testcase":"default value check","req":"no"}
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

    if rectype == 'h':   
        logFormat = generate_custom_grok_pattern(ads_18a_header_rec)
        jsondata = ads_18a_header_rec
        rec_val = ' '
    if rectype == 'd':
        #logFormat = generate_custom_grok_pattern(ads_18a_detail_rec)
        jsondata = ads_18a_detail_rec
        rec_val = '01'
    if rectype == 't':
        logFormat = generate_custom_grok_pattern(ads_18a_header_rec)
        jsondata = ads_18a_header_rec
        rec_val = 'T'
    
    
    fwf_data=[]
    error_data=[]
    header_txt = str("{:<20}".format("Testcase_number")+"|"+"{:<20}".format("Total_Records_Count")+"|"+"{:<40}".format("Column")+"|"+"{:<10}".format("Pos")+"|"+"{:<30}".format("Default_values")+"|"+"{:<20}".format("Passed_record_count")+"|"+"{:<20}".format("Failed_record_count")+"|"+"{:<10}".format("Test_Status") +"|"+"Test_Case")
    fwf_data.append(header_txt)
    
    error_recs = "Testcase_number|CNTRCT_NUM|Value_in_File|Field|Test_Case_Description"
    
    error_data.append(error_recs)
    
    def return_testcase_res(test_case_number,totalrecs,columnname,pos,defaultvalues,passedrecs,failedrecs,teststatus,testcase):
        return str("{:<20}".format(test_case_number)+"|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(columnname)+"|"+"{:<10}".format(pos)+"|"+"{:<30}".format(str(defaultvalues))+"|"+"{:<20}".format(str(passedrecs))+"|"+"{:<20}".format(failedrecs)+"|"+"{:<10}".format(teststatus)+"|"+"{:<200}".format(testcase))
        
    
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
                        error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTRCT_NUM,'|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")+" limit 100").rdd.flatMap(lambda x:x).collect())
                    
                    
                txt = str("{:<20}".format(test_case_number)+ "|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(c)+"|"+"{:<10}".format(jsondata[c]['pos'])+"|"+"{:<30}".format(str(jsondata[c]['default_val']))+ "|"+"{:<20}".format(passedrecs)+"|"+"{:<20}".format(failedrecs) +"|"+"{:<10}".format(teststatus) + "|"+"{:<200}".format(test_case))
                fwf_data.append(txt)    
        
        if (jsondata[c]['default_val'] == 'NA' and jsondata[c]['req'] == 'yes'):
            test_case=jsondata[c]['testcase']
            #test_case= "Required field validation"
            test_case_number=str(jsondata[c]['testcasenumber'])
            
            print ("select count(1) from tgtdata where "+ c +" is null or trim("+ c +") = '' ".format(c))
            
            test_case_count = str(spark.sql('''select count(1) from tgtdata where "+ c +" is null or trim("+ c +") = '' '''.format(c)).collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
                
            if (int(test_case_count)>0):
                error_data.extend(spark.sql("select concat("+test_case_number+", '|', CNTR_NUM, '|', "+c+", '|', '"+c+"' ) as Res from tgtdata where "+ c +" is null or trim("+ c +") = '' ".format(c)+" limit 100").rdd.flatMap(lambda x:x).collect())
        
            txt = str("{:<20}".format(test_case_number)+ "|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(c)+"|"+"{:<10}".format(jsondata[c]['pos'])+"|"+"{:<30}".format(str(jsondata[c]['default_val']))+ "|"+"{:<20}".format(passed_recs)+"|"+"{:<20}".format(test_case_count) +"|"+"{:<10}".format(teststatus) + "|"+"{:<200}".format(test_case))
            fwf_data.append(txt)
        
        
        
        # check source to target missing CNTRCT_NUM
        if (c =='CNTRCT_NUM'):
            
            test_case_number='10'
            test_case_desc = 'check source to target missing CNTRCT_NUM'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTRCT_NUM
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(s.CNTRCT_NUM)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTRCT_NUM,'|',"+c+",'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTRCT_NUM
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'1-6','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
    
    

        # check source to target missing CNTRCT_NUM, CNTRCT_SUBDIV_CD
        if (c =='CNTRCT_SUBDIV_CD'):
            
            test_case_number='11'
            test_case_desc = 'check source to target missing CNTRCT_NUM, CNTRCT_SUBDIV_CD -- exists'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTRCT_NUM, s.CNTRCT_SUBDIV_CD
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.CNTRCT_SUBDIV_CD) = trim(s.CNTRCT_SUBDIV_CD)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(s.CNTRCT_NUM,s.CNTRCT_SUBDIV_CD)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.CNTRCT_SUBDIV_CD) = trim(s.CNTRCT_SUBDIV_CD)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                test_case_desc = 'check source to target missing CNTRCT_NUM, CNTRCT_SUBDIV_CD -- not exists'
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTRCT_NUM,'|',"+c+",'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.CNTRCT_SUBDIV_CD) = trim(s.CNTRCT_SUBDIV_CD)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTRCT_NUM, s.CNTRCT_SUBDIV_CD
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.CNTRCT_SUBDIV_CD) = trim(s.CNTRCT_SUBDIV_CD))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'8-11','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   

       
        
        # check source to target missing CNTRCT_NUM, INT_RTE
        if (c =='INT_RTE'):
            
            test_case_number='12'
            test_case_desc = 'check source to target missing CNTRCT_NUM,INT_RTE'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTRCT_NUM, s.INT_RTE
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id)
    					and trim(t.INT_RTE) = trim(s.INT_RTE)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(s.CNTRCT_NUM,s.INT_RTE)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id)
    					and trim(t.INT_RTE) = trim(s.INT_RTE)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTRCT_NUM,'|',"+c+",'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id) and trim(t.INT_RTE) = trim(s.INT_RTE)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTRCT_NUM,trim(s.EASY_Group_Client_Id) as EASY_Group_Client_Id, s.INT_RTE
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id) and trim(t.INT_RTE) = trim(s.INT_RTE))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'36-42','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   

        
        
        
        # check source to target missing CNTRCT_NUM, EASY_Group_Client_Id
        if (c =='EASY_Group_Client_Id'):
            
            test_case_number='13'
            test_case_desc = 'check source to target missing CNTRCT_NUM,EASY_Group_Client_Id'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTRCT_NUM, s.EASY_Group_Client_Id
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(s.CNTRCT_NUM,s.EASY_Group_Client_Id)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTRCT_NUM,'|',"+c+",'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTRCT_NUM
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.EASY_Group_Client_Id) = trim(s.EASY_Group_Client_Id))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'44-53','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   

        
        
        
        # check source to target missing CNTRCT_NUM, EASY_Group_Account_Id
        if (c =='EASY_Group_Account_Id'):
            
            test_case_number='14'
            test_case_desc = 'check source to target missing CNTRCT_NUM,EASY_Group_Account_Id'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTRCT_NUM, s.EASY_Group_Account_Id
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.EASY_Group_Account_Id) = trim(s.EASY_Group_Account_Id)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(s.CNTRCT_NUM,s.EASY_Group_Account_Id)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.EASY_Group_Account_Id) = trim(s.EASY_Group_Account_Id)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTRCT_NUM,'|',"+c+",'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.EASY_Group_Account_Id) = trim(s.EASY_Group_Account_Id)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTRCT_NUM,s.EASY_Group_Account_Id
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.EASY_Group_Account_Id) = trim(s.EASY_Group_Account_Id))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'55-67','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt) 
        
        
        
        # check source to target missing CNTRCT_NUM, SDIO_ID
        if (c =='SDIO_ID'):
            
            test_case_number='15'
            test_case_desc = 'check source to target missing CNTRCT_NUM, SDIO_ID'
            print(test_case_desc)
            spark.sql('''select distinct s.CNTRCT_NUM, s.SDIO_ID
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.SDIO_ID) = trim(s.SDIO_ID)
    					) ''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(s.CNTRCT_NUM,s.SDIO_ID)
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM)
    					and trim(t.SDIO_ID) = trim(s.SDIO_ID)
    					) ''').collect()[0][0])
            
            passed_recs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                test_case_desc = 'check source to target missing CNTRCT_NUM, SDIO_ID-- no exists'
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+test_case_number+",'|',CNTRCT_NUM,'|',"+c+",'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.SDIO_ID) = trim(s.SDIO_ID)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.CNTRCT_NUM, s.SDIO_ID
                        from srcdata s  where not exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTRCT_NUM) = trim(s.CNTRCT_NUM) and trim(t.SDIO_ID) = trim(s.SDIO_ID))''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'69-74','NA',str(passed_recs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt) 
        
        


    print("saving......")        
    
    s3Client=boto3.client('s3')

    if rectype == 'h':   
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ADS_18a_Header_Record_TestResult_{date_file}.TXT')    
        if len(error_data) > 0:
            
            print("header records errors start here")
            print(error_data)
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ADS_18a_Header_Record_TestResult_Failures_{date_file}.TXT')
        
            print("details records errors end here")
    if rectype == 'd':
        print("save details records")
        
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ADS_18a_Detail_Record_TestResult_{date_file}.TXT')
        
        print("save details records end")
        
        if len(error_data) > 0:
            
            print("details records errors start here")
            print(error_data)
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ADS_18a_Detail_Record_TestResult_Failures_{date_file}.TXT')
        
            print("details records errors end here")
            
    if rectype == 't':
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'ADS_18a_Trailer_Record_RestResult_{date_file}.TXT')
        
        if len(error_data) > 0:
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ADS_18a_Trailer_Record_TestResult_Failure_Recs_{date_file}.txt')
                  
                
    
except Exception as e:
    print("Exception Error:", e)
    

job.commit()




-------------------------------------------------------------------------------------------------------



Test_Sap_io_Annual_UAT1
--------------------------------------------------------



import re
import sys
import time
import boto3
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


s3uri="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/sap-io_Annual/01022025 Preprod validation/DBRK_IO00124.TXT"


s3 = boto3.resource('s3')
#tgt_bucket='uat1-gwf-cc-cats-publish-class0-us-east-1'
tgt_bucket='uat1-gwf-cc-cats-filexfer-us-east-1'
#tgt_key='release01/2023-12-07/'
tgt_key='outbound/CaTS_Automation_Testing/sap-io_Annual/'


cur_date=str(time.strftime('%Y%m%d'))
day_of_year=datetime.now().timetuple().tm_yday
year=str(time.strftime('%y'))
date_file=str(time.strftime('%Y%m%d%H%M%S'))


test_case_number = 0

src_bucket = 'uat1-gwf-cc-cats-filexfer-us-east-1'
src_key= 'inbound/telus/'
#########################################
# Annual Source Files

Monthly_Pregexp = 'P_DB_PAYMENT_PAYMENTS_M\d{2}_\d{8}_\d{6}'
Monthly_Dregexp = 'DB_PAYMENT_DEPOSIT_M\d{2}_\d{8}_\d{6}'
#########################################

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

try:
    # Source Data
    
    
    # ######################################################################################
    # payments
    
    print("Source data EmployeeProfileDataForYearlyUpdates")
    EMPLOYEEPROFILEDATAFORYEARLTUPDATES_PATH = "s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/sap-io_Annual/01022025 Preprod validation/EmployeeProfileDataForYearlyUpdates.csv" 
    EMPLOYEEPROFILEDATAFORYEARLTUPDATES_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{EMPLOYEEPROFILEDATAFORYEARLTUPDATES_PATH}")
    EMPLOYEEPROFILEDATAFORYEARLTUPDATES_DF.createOrReplaceTempView("EmployeeProfileDataForYearlyUpdates")
    print(EMPLOYEEPROFILEDATAFORYEARLTUPDATES_PATH)
    print(EMPLOYEEPROFILEDATAFORYEARLTUPDATES_DF.printSchema())
    spark.sql("select * from  EmployeeProfileDataForYearlyUpdates limit 30").show(truncate=0)
    
      
    # ######################################################################################
    
    
    # PAYEE_LOOKUP
    
    print("Source data PAYEE_LOOKUP")
    PAYEE_LOOKUP_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/sap-io_Annual/01022025 Preprod validation/CaTS_Payee_Lookup.csv" #Monthly_PROD_validation
    PAYEE_LOOKUP_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{PAYEE_LOOKUP_PATH}")
    PAYEE_LOOKUP_DF.createOrReplaceTempView("payee_lookup")
    print(PAYEE_LOOKUP_DF.printSchema())
    spark.sql("select * from  payee_lookup limit 30").show(truncate=0)
    

    # ######################################################################################
    
    print("Source data")
    
    df_src = spark.sql('''
        SELECT DISTINCT 
                a.TaxID,
                a.payeeid,
                a.PayeeStatus,
                a.PensionPlan,
                c.businesspartner,
                c.ga_id_ssn_payeeid_combo,
                case when a.PensionPlan = 'QUAL-PEN' or a.PensionPlan = 'NQUAL-PEN' then 'DR' else 'DL' end as INSOBJECTTYP,
                case when a.PensionPlan = 'QUAL-PEN' or a.PensionPlan = 'NQUAL-PEN' then CONCAT('DB_RECPYT_',c.businesspartner) else CONCAT('DB_SNGPYT_',c.businesspartner) end as INSOBJECT,
                case when a.PayeeStatus = 'IC' or a.PayeeStatus = 'DI' then 'Y'
                when (a.PayeeStatus = 'AP' or a.PayeeStatus = 'AS') AND (a.PensionPlan = 'QUAL-NE-CASH' or a.PensionPlan = 'QUAL-RE-CASH' or a.PensionPlan = 'QUAL-D-ROLL' or a.PensionPlan = 'QUAL-R-ROLL' or a.PensionPlan = 'NQUAL-CASH') then 'Y' else ' ' end as ZTOTDISTFLAG
        FROM
        EmployeeProfileDataForYearlyUpdates as a 
            inner join payee_lookup as c ON 
            CONCAT(lpad(a.taxid,9,'0'),'-',a.payeeid) = CONCAT(split(c.GA_ID_SSN_PAYEEID_COMBO,'\\\\-')[2],'-',split(c.GA_ID_SSN_PAYEEID_COMBO,'\\\\-')[3])''')
    df_src.show(80,truncate=0)
    df_src.createOrReplaceTempView("srcdata")
    print("F1........End")
    print("count from source ="+str(df_src.count()))
    
    
    Save_PATH ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/sap-io_Annual/SAP_IO_Annual_src_data/"
    df_src.coalesce(1).write.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).mode("overwrite").save(f"{Save_PATH}")
    
    sap_io_header_rec = {
    'RECTYP':{'testcasenumber':1,'pos':'1-1','len':1,'default':'yes','default_val':'s_1','testcase':'default value check','req':'yes'},
    'SYCO':{'testcasenumber':2,'pos':'2-5','len':4,'default':'yes','default_val':'DBRK','testcase':'default value check','req':'yes'},
    'CYCLE':{'testcasenumber':3,'pos':'6-10','len':5,'default':'no','default_val':'NA','testcase':'default value check','req':'yes'},
    'FLID':{'testcasenumber':4,'pos':'11-45','len':35,'default':'no','default_val':'NA','testcase':'default value check','req':'yes'},
    'FDATE':{'testcasenumber':5,'pos':'46-53','len':8,'default':'no','default_val':'NA','testcase':'default value check','req':'yes'},
    'FTIME':{'testcasenumber':6,'pos':'54-59','len':6,'default':'no','default_val':'NA','testcase':'default value check','req':'yes'},
    'FILLER':{'testcasenumber':7,'pos':'60-235','len':176,'default':'no','default_val':'NA','testcase':'default value check','req':'yes'}

    }
    

    sap_io_detail_rec =  {
        "RECTYP":{"testcasenumber":8,"pos":"1-1","len":1,"default":"yes","default_val":"B","testcase":"default value check","req":"yes"},
        "AKTYP":{"testcasenumber":9,"pos":"2-3","len":2,"default":"yes","default_val":"04","testcase":"default value check","req":"yes"},
        "PART_AKTYPE":{"testcasenumber":10,"pos":"4-4","len":1,"default":"yes","default_val":"M","testcase":"default value check","req":"yes"},
        "PARTNER":{"testcasenumber":11,"pos":"5-14","len":10,"default":"no","default_val":"NA","testcase":"default value check","req":"yes"},
        "INSOBJECTTYP":{"testcasenumber":12,"pos":"15-16","len":2,"default":"no","default_val":"NA","testcase":"default value check","req":"no"},
        "INSOBJECT":{"testcasenumber":13,"pos":"17-36","len":20,"default":"no","default_val":"NA","testcase":"default value check","req":"yes"},
        "ZPAYEEID":{"testcasenumber":14,"pos":"37-56","len":20,"default":"no","default_val":"NA","testcase":"default value check","req":"yes"},
        "ZPAYMENTID":{"testcasenumber":15,"pos":"57-92","len":36,"default":"yes","default_val":"sl_35","testcase":"default value check","req":"no"},
        "ZTAXLOC":{"testcasenumber":16,"pos":"93-102","len":10,"default":"yes","default_val":"sl_9","testcase":"default value check","req":"no"},
        "ZTAXCOUNTRY":{"testcasenumber":17,"pos":"103-105","len":3,"default":"yes","default_val":"sl_2","testcase":"default value check","req":"no"},
        "ZIRSDISTCD":{"testcasenumber":18,"pos":"106-107","len":2,"default":"yes","default_val":"sl_1","testcase":"default value check","req":"no"},
        "ZNRESIDENTOPTION":{"testcasenumber":19,"pos":"108-110","len":3,"default":"yes","default_val":"sl_2","testcase":"default value check","req":"no"},
        "ZNRESIDENTRATE":{"testcasenumber":20,"pos":"111-116","len":6,"default":"yes","default_val":"sl_5","testcase":"default value check","req":"no"},
        "ZFELECTOPT":{"testcasenumber":21,"pos":"117-119","len":3,"default":"yes","default_val":"sl_2","testcase":"default value check","req":"no"},
        "ZFFILSTAT":{"testcasenumber":22,"pos":"120-120","len":1,"default":"yes","default_val":"/","testcase":"default value check","req":"no"},
        "ZW4JPF":{"testcasenumber":23,"pos":"121-121","len":1,"default":"yes","default_val":"/","testcase":"default value check","req":"no"},
        "ZW4PJPT":{"testcasenumber":24,"pos":"122-136","len":15,"default":"yes","default_val":"sl_14","testcase":"default value check","req":"no"},
        "ZW4PDA":{"testcasenumber":25,"pos":"137-151","len":15,"default":"yes","default_val":"sl_14","testcase":"default value check","req":"no"},
        "ZW4POTHINC":{"testcasenumber":26,"pos":"152-166","len":15,"default":"yes","default_val":"sl_14","testcase":"default value check","req":"no"},
        "ZW4PDED":{"testcasenumber":27,"pos":"167-181","len":15,"default":"yes","default_val":"sl_14","testcase":"default value check","req":"no"},
        "ZFTWHVA":{"testcasenumber":28,"pos":"182-196","len":15,"default":"yes","default_val":"sl_14","testcase":"default value check","req":"no"},
        "ZFLSRATE":{"testcasenumber":29,"pos":"197-202","len":6,"default":"yes","default_val":"sl_5","testcase":"default value check","req":"no"},
        "ZSELECTOPT":{"testcasenumber":30,"pos":"203-205","len":3,"default":"yes","default_val":"sl_2","testcase":"default value check","req":"no"},
        "ZSFILSTAT":{"testcasenumber":31,"pos":"206-206","len":1,"default":"yes","default_val":"/","testcase":"default value check","req":"no"},
        "ZSALLOWANCES":{"testcasenumber":32,"pos":"207-208","len":2,"default":"yes","default_val":"sl_1","testcase":"default value check","req":"no"},
        "ZSTWHAA":{"testcasenumber":33,"pos":"209-223","len":15,"default":"yes","default_val":"sl_14","testcase":"default value check","req":"no"},
        "ZSTWHAP":{"testcasenumber":34,"pos":"224-229","len":6,"default":"yes","default_val":"sl_5","testcase":"default value check","req":"no"},
        "ZSWHRATE":{"testcasenumber":35,"pos":"230-235","len":6,"default":"yes","default_val":"sl_5","testcase":"default value check","req":"no"},
        "ZTOTDISTFLAG":{"testcasenumber":36,"pos":"236-236","len":1,"default":"no","default_val":"NA","testcase":"default value check","req":"no"}
    }
    
    sap_io_trailer_rec = {
        'Trailer Record Type':{'testcasenumber':36,'pos':'1-1','len':1,'default':'yes','default_val':'T','testcase':'default value check','req':'no'},
        'Trailer Record Count':{'testcasenumber':37,'pos':'2-12','len':11,'default':'no','default_val':'NA','testcase':'default value check','req':'no'},
        'Trailer Company Amount':{'testcasenumber':38,'pos':'13-29','len':17,'default':'no','default_val':'NA','testcase':'default value check','req':'no'},
        'Trailer':{'testcasenumber':39,'pos':'30-235','len':206,'default':'no','default_val':'NA','testcase':'default value check','req':'no'}
    
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
    rec_val = 'B'
    
    if rectype == 'h':
        logFormat = generate_custom_grok_pattern(sap_io_header_rec)
        jsondata =sap_io_header_rec
        print(jsondata)
        rec_val = ' '
    if rectype == 'd':
        logFormat = generate_custom_grok_pattern(sap_io_detail_rec)
        jsondata = sap_io_detail_rec
        print(jsondata)
        rec_val = 'B'
    if rectype == 't':
        logFormat = generate_custom_grok_pattern(sap_io_trailer_rec)
        jsondata =sap_io_trailer_rec
        rec_val = 'T'
    
    
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
    
    if rectype == 'h':
        detailed_rec_cnt = sourceDF1.filter((sourceDF1.RECTYP== rec_val)).count()
        sourceDF = sourceDF1.filter((sourceDF1.RECTYP== rec_val))
        sourceDF.cache()
        sourceDF.show(truncate=0)
    if rectype == 'd':
        detailed_rec_cnt = sourceDF1.filter((sourceDF1.RECTYP== rec_val)).count()
        sourceDF = sourceDF1.filter((sourceDF1.RECTYP== rec_val))
        sourceDF.cache()
        sourceDF.show(truncate=0)
        
    if rectype == 't':
        detailed_rec_cnt = sourceDF1.filter((sourceDF1.Trailer== rec_val)).count()
        sourceDF = sourceDF1.filter((sourceDF1.Trailer== rec_val))
        sourceDF.cache()
        sourceDF.show(truncate=0)
        
    
    print("\n Target Data.......")
    
    def return_testcase_res(test_case_number,totalrecs,columnname,pos,defaultvalues,passedrecs,failedrecs,teststatus,testcase):
        return str("{:<20}".format(test_case_number)+"|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(columnname)+"|"+"{:<10}".format(pos)+"|"+"{:<30}".format(str(defaultvalues))+"|"+"{:<20}".format(str(passedrecs))+"|"+"{:<20}".format(failedrecs)+"|"+"{:<10}".format(teststatus)+"|"+"{:<200}".format(testcase))
    
    
    #Target temp table
    sourceDF.createOrReplaceTempView("tgtdata")
    
    totalrecs=str(spark.sql("select count(1) from tgtdata").collect()[0][0])
    
    print("Total target records....."+str(totalrecs))
    
    
    fwf_data=[]
    error_data=[]
    header_txt = str("{:<20}".format("Testcase_number")+"|"+"{:<20}".format("Total_Records_Count")+"|"+"{:<40}".format("Column")+"|"+"{:<10}".format("Pos")+"|"+"{:<30}".format("Default_values")+"|"+"{:<20}".format("Passed_record_count")+"|"+"{:<20}".format("Failed_record_count")+"|"+"{:<10}".format("Test_Status") +"|"+"Test_Case")
    fwf_data.append(header_txt)
    
    error_recs = "Testcase_number|PayeeID|Value_in_File|Field"
    
    error_data.append(error_recs)
    
    for c in jsondata:
        
        if (jsondata[c]['default'] == 'yes'):
            test_case=jsondata[c]['testcase']
            test_case_number = test_case_number + 1
            # default values
            lst = jsondata[c]['default_val'].split('|')
            lst_all_val = []
            for s in lst:
                if 's_' in s:
                    e_str=''
                    for i in range(0,int(s.replace('s_',''))):
                        e_str+=' '    
                    lst_all_val.append(e_str)
                elif 'sl_' in s:
                    e_str=''
                    for i in range(0,int(s.replace('sl_',''))):
                        e_str+=' '
                    lst_all_val.append('/'+e_str)
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
                    error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")+" limit 100" ).rdd.flatMap(lambda x:x).collect())
                    
                    spark.sql("select concat("+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")).show(truncate=0)
                    
                if rectype == 'd':  
                    error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',ZPayeeID, '|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")+" limit 100").rdd.flatMap(lambda x:x).collect())
                
                    #spark.sql("select concat(Shareholder_Account_Number,'|',"+c+",'|','"+c+"') as Res from tgtdata where "+ c +" not in {}".format(tuple(lst_all_val) if len(lst_all_val) !=1 else "('"+ str(lst_all_val[0]) +"')")).show(truncate=0)
                
            txt = str("{:<20}".format(str(test_case_number))+ "|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(c)+"|"+"{:<10}".format(jsondata[c]['pos'])+"|"+"{:<30}".format(str(jsondata[c]['default_val']))+ "|"+"{:<20}".format(passedrecs)+"|"+"{:<20}".format(failedrecs) +"|"+"{:<10}".format(teststatus) + "|"+"{:<200}".format(test_case))
            
            fwf_data.append(txt)  
                
        #Required field check
        if (jsondata[c]['default_val'] == 'NA' and jsondata[c]['req'] == 'yes'):
            test_case=jsondata[c]['testcase']
            #test_case= "Null check"
            test_case_number = test_case_number + 1
            
            print ("select count(1) from tgtdata where "+ c +" is null or trim("+ c +") = '' ".format(c))
            print("This is Required field check start")
            test_case_count = str(spark.sql('''select count(1) from tgtdata where "+ c +" is null or trim("+ c +") = '' '''.format(c)).collect()[0][0])
            
            
            print("This is Required field check end")
            
            passedrecs=int(totalrecs)-int(test_case_count)
            
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
                
            if (int(test_case_count)>0):
                error_data.extend(spark.sql("select concat("+str(test_case_number)+", '|', ZPAYEEID,'|',PARTNER,'|', "+c+", '|', '"+c+"' ) as Res from tgtdata where "+ c +" is null or trim("+ c +") = '' ".format(c)+" limit 100").rdd.flatMap(lambda x:x).collect())
        
            txt = str("{:<20}".format(str(test_case_number))+ "|"+"{:<20}".format(totalrecs)+ "|"+"{:<40}".format(c)+"|"+"{:<10}".format(jsondata[c]['pos'])+"|"+"{:<30}".format(str(jsondata[c]['default_val']))+ "|"+"{:<20}".format(passedrecs)+"|"+"{:<20}".format(test_case_count) +"|"+"{:<10}".format(teststatus) + "|"+"{:<200}".format(test_case))
            fwf_data.append(txt)
                
        
        # ######################################################################################
             
        # check source to target missing Business_Partner_ID
        
        if (c =='PARTNER'):
            
            test_case_number = test_case_number + 1
            
            test_case_desc = 'Source to target missing Business_Partner_ID'
            print(test_case_desc)
            spark.sql('''select distinct s.payeeid, s.INSOBJECT from srcdata as s inner join tgtdata as t on trim(s.payeeid) = trim(t.zpayeeid) and trim(S.INSOBJECT) = trim(t.INSOBJECT) where trim(s.businesspartner) <> trim(t.partner)''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(*) from (select distinct s.payeeid,s.INSOBJECT from srcdata as s inner join tgtdata as t on trim(s.payeeid) = trim(t.zpayeeid) and trim(S.INSOBJECT) = trim(t.INSOBJECT) where trim(s.businesspartner) <> trim(t.partner)) ''').collect()[0][0])

            
            passedrecs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',PayeeID,'|', '"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.PARTNER) = trim(s.businesspartner)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.payeeid, s.businesspartner from srcdata as s inner join tgtdata as t on trim(s.payeeid) = trim(t.zpayeeid) and trim(S.INSOBJECT) = trim(t.INSOBJECT)where trim(s.businesspartner) <> trim(t.partner)''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'37-56','NA',str(passedrecs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
        
        # ######################################################################################
        # check source to target missing Business_Partner_ID
        
        if (c =='PARTNER'):
            
            test_case_number = test_case_number + 1
            
            test_case_desc = 'Source to target missing Business_Partner_ID_2'
            print(test_case_desc)
            spark.sql('''select DISTINCT s.taxid, s.payeeid, s.ga_id_ssn_payeeid_combo, s.businesspartner from srcdata as s inner join tgtdata as t on trim(s.payeeid)=trim(t.zpayeeid) and trim(s.INSOBJECT) = trim(t.INSOBJECT) where trim(s.businesspartner)=trim(t.partner)''').show(truncate=0)
            
            test_case_count=str(spark.sql('''select count(*) from (select distinct s.taxid, s.payeeid, s.ga_id_ssn_payeeid_combo, s.businesspartner from srcdata as s inner join tgtdata as t on trim(s.payeeid)=trim(t.zpayeeid) and trim(s.INSOBJECT) = trim(t.INSOBJECT) where trim(s.businesspartner)<>trim(t.partner))''').collect()[0][0])
            
            passedrecs=int(totalrecs)-int(test_case_count)
            
            print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
            if (int(test_case_count)>0):
                teststatus = "Fail"
            else:
                teststatus = "Pass"
            
            if (int(test_case_count)>0):
                
                print(test_case_desc)
                
                error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',PayeeID,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where trim(t.PARTNER) = trim(s.businesspartner)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                spark.sql('''select distinct s.payeeid from srcdata as s inner join tgtdata as t on trim(s.payeeid)=trim(t.zpayeeid) and trim(S.INSOBJECT) = trim(t.INSOBJECT) where trim(s.businesspartner)<>trim(t.partner)''').show(truncate=0)
                    
            txt = return_testcase_res(test_case_number,totalrecs,c,'3-8','NA',str(passedrecs),str(test_case_count),teststatus,test_case_desc)
            
            fwf_data.append(txt)   
        
        # ######################################################################################
        
        # check source to target missing ZPAYEEID
        if (c =='ZPAYEEID'):
            
             test_case_number = test_case_number + 1
             test_case_desc = 'Source to target missing PayeeID'
             print(test_case_desc)

            

             spark.sql('''select distinct s.payeeid, s.businesspartner from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) where trim(s.payeeid) <> trim(t.zpayeeid) ''').show(truncate=0)
                      
             test_case_count=str(spark.sql('''select count(*) from (select distinct s.payeeid, s.businesspartner from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) where trim(s.payeeid) <> trim(t.zpayeeid)) ''').collect()[0][0])
    					
             passedrecs=int(totalrecs)-int(test_case_count)
            
             print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
             if (int(test_case_count)>0):
                 teststatus = "Fail"
             else:
                 teststatus = "Pass"
            
             if (int(test_case_count)>0):
                
                 print(test_case_desc)
                
                 error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',PayeeID,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where  trim(s.PayeeID) <> trim(t.ZPAYEEID)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                 spark.sql('''select distinct s.payeeid from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) where trim(s.payeeid) <> trim(t.zpayeeid))''').show(truncate=0)
                    
             txt = return_testcase_res(test_case_number,totalrecs,c,'3-8','NA',str(passedrecs),str(test_case_count),teststatus,test_case_desc)
            
             fwf_data.append(txt)   

        # ######################################################################################
        
        # Data Validation for the column INSOBJECTTYP
        if (c =='INSOBJECTTYP'):
            
             test_case_number = test_case_number + 1
             test_case_desc = 'Data Validation for the column INSOBJECTTYP'
             print(test_case_desc)
             
             spark.sql('''select distinct s.payeeid, s.businesspartner,s.INSOBJECTTYP from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.payeeid) = trim(t.zpayeeid) where trim(s.INSOBJECTTYP)<>trim(t.INSOBJECTTYP) ''').show(truncate=0)
                      
             test_case_count=str(spark.sql('''select count(*) from (select distinct s.payeeid, s.businesspartner from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.payeeid) = trim(t.zpayeeid) where trim(s.INSOBJECTTYP) <> trim(t.INSOBJECTTYP))''').collect()[0][0])
            
             passedrecs=int(totalrecs)-int(test_case_count)
            
             print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
             if (int(test_case_count)>0):
                 teststatus = "Fail"
             else:
                 teststatus = "Pass"
            
             if (int(test_case_count)>0):
                
                 print(test_case_desc)
                
                 error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',PayeeID,'|',INSOBJECTTYP,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where  trim(s.INSOBJECTTYP) <> trim(t.INSOBJECTTYP)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                 spark.sql('''select distinct s.PayeeID from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.PayeeID) = trim(t.ZPAYEEID) where trim(S.INSOBJECTTYP) = trim(t.INSOBJECTTYP)''').show(truncate=0)
                    
             txt = return_testcase_res(test_case_number,totalrecs,c,'57-92','NA',str(passedrecs),str(test_case_count),teststatus,test_case_desc)
            
             fwf_data.append(txt)   

        # ######################################################################################
        
        # Data Validation for the column INSOBJECT
        if (c =='ZTAXLOC'):
            
             test_case_number = test_case_number + 1
             test_case_desc = 'Data Validation for the column INSOBJECT '
             print(test_case_desc)
             spark.sql('''select distinct s.payeeid, s.businesspartner, s.INSOBJECT from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.PayeeID) = trim(t.ZPAYEEID) where trim(s.INSOBJECT) <> trim(t.INSOBJECT)''').show(truncate=0)
            
             test_case_count=str(spark.sql('''select count(*) from (select distinct s.payeeid, s.businesspartner, s.INSOBJECT from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.PayeeID) = trim(t.ZPAYEEID) where trim(s.INSOBJECT) <> trim(t.INSOBJECT))''').collect()[0][0])
            
             passedrecs=int(totalrecs)-int(test_case_count)
            
             print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
             if (int(test_case_count)>0):
                 teststatus = "Fail"
             else:
                 teststatus = "Pass"
            
             if (int(test_case_count)>0):
                
                 print(test_case_desc)
                
                 error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',PayeeID,'|',INSOBJECT,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where  trim(s.INSOBJECT) <> trim(t.INSOBJECT)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                 spark.sql('''select distinct s.PayeeID from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.PayeeID) = trim(t.ZPAYEEID) where trim(t.INSOBJECT) = trim(s.INSOBJECT))''').show(truncate=0)
                    
             txt = return_testcase_res(test_case_number,totalrecs,c,'93-102','NA',str(passedrecs),str(test_case_count),teststatus,test_case_desc)
            
             fwf_data.append(txt)   

        # ######################################################################################
        
        # Data validation for the column ZTOTDISTFLAG
        if (c =='ZTOTDISTFLAG'):
            
             test_case_number = test_case_number + 1
             test_case_desc = 'Data Validation for the column ZTOTDISTFLAG'
             print(test_case_desc)
             spark.sql('''select distinct s.payeeid,s.businesspartner, s.ZTOTDISTFLAG from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.payeeid) = trim(t.ZPAYEEID) where trim(S.ZTOTDISTFLAG) <> trim(t.ZTOTDISTFLAG)''').show(truncate=0)
            
             test_case_count=str(spark.sql('''select count(*) from (select distinct s.payeeid, s.businesspartner, s.ZTOTDISTFLAG from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.payeeid) = trim(t.ZPAYEEID) where trim(S.ZTOTDISTFLAG) <> trim(t.ZTOTDISTFLAG))''').collect()[0][0])
            
             passedrecs=int(totalrecs)-int(test_case_count)
            
             print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
            
             if (int(test_case_count)>0):
                 teststatus = "Fail"
             else:
                 teststatus = "Pass"
            
             if (int(test_case_count)>0):
                
                 print(test_case_desc)
                
                 error_data.extend(spark.sql("select concat("+str(test_case_number)+",'|',PayeeID,'|',ZTOTDISTFLAG,'|','"+c+"','|','"+test_case_desc+"') as Res from srcdata s  where not exists (select '1' from tgtdata t where  trim(s.ZTOTDISTFLAG) <> trim(t.ZTOTDISTFLAG)) limit 100").rdd.flatMap(lambda x:x).collect())
                
                 spark.sql('''select distinct s.PayeeID,s.ZTOTDISTFLAG from srcdata as s inner join tgtdata as t on trim(s.businesspartner) = trim(t.partner) and trim(s.PayeeID) = trim(t.ZPAYEEID) where trim(t.ZTOTDISTFLAG) = trim(s.ZTOTDISTFLAG)''').show(truncate=0)
                    
             txt = return_testcase_res(test_case_number,totalrecs,c,'103-105','NA',str(passedrecs),str(test_case_count),teststatus,test_case_desc)
            
             fwf_data.append(txt)   

        # ######################################################################################
 
        
    print("saving......")        
    
    s3Client=boto3.client('s3')
    
    if rectype == 'h':
        print("save details records")
        
    s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'SAP_IO_Header_Record_TestResult_{date_file}.TXT')
        
    print("save details records end")
        
    if len(error_data) > 0:
        print("details records errors start here")
        print(error_data)
        s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'SAP_IO_Header_Record_TestResult_Failures_{date_file}.TXT')
        print("details records errors end here")
    
    if rectype == 'd':
        print("save details records")
        
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'SAP_IO_Detail_Record_TestResult_{date_file}.TXT')
        
        print("save details records end")
        
        if len(error_data) > 0:
            
            print("details records errors start here")
            print(error_data)
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'SAP_IO_Detail_Record_TestResult_Failures_{date_file}.TXT')
        
            print("details records errors end here")
            
    if rectype == 't':
        print(type(fwf_data))
        print(fwf_data)
        s3Client.put_object(Body='\n'.join(fwf_data), Bucket=tgt_bucket, Key=tgt_key+f'SAP_IO_Trailer_Record_RestResult_{date_file}.TXT')
        
        if len(error_data) > 0:
            s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'SAP_IO_Trailer_Record_TestResult_Failure_Recs_{date_file}.txt')
            
            
except Exception as e:
    print("Exception Error:", e)


job.commit()          
    




