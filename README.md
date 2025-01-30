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

rectype ='d'
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
    print("A063PI count", str(phoenix_transaction_history_df.count()))
    


    # ######################################################################################
    # erf_contracts_w_Fund_ID_with_GG_Lookup
    
    print("Source data erf_contracts_w_Fund_ID_with_GG_Lookup")
    # erf_contracts_w_Fund_ID_with_GG_Lookup_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/erf_contracts_w_Fund_ID_with_GG_Lookup.csv"
    erf_contracts_w_Fund_ID_with_GG_Lookup_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/archive/2025-01-21/ERF1_PHX_TO_ERF/Daily/Run_no6/erf_contracts_w_Fund_ID_with_GG_Lookup.csv"
    erf_contracts_w_Fund_ID_with_GG_DF = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter='|', quote='"', escape='"', skip_blank_lines=True).load(f"{erf_contracts_w_Fund_ID_with_GG_Lookup_path}")
    erf_contracts_w_Fund_ID_with_GG_DF.createOrReplaceTempView("erf1_erf_contracts_with_fund_id_gg_lookup")
    print(erf_contracts_w_Fund_ID_with_GG_DF.printSchema())
    # spark.sql("select * from erf1_erf_contracts_with_fund_id_gg_lookup limit 30").show(truncate=0)
    
    
    # ######################################################################################
    # ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds
    
    print("Source data ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds")
    # ttc_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/outbound/CaTS_Automation_Testing/erf1/ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds.csv"
    ttc_path ="s3://uat1-gwf-cc-cats-filexfer-us-east-1/archive/2025-01-21/ERF1_PHX_TO_ERF/Daily/Run_no6/ERF_Cat_Code_and_Cat_Item_Code_4_PARIS_TTC_w_mapping_2_Empower_Cds.csv"
    ttc_path_df = spark.read.format('com.databricks.spark.csv').options(header='true', delimiter=',', quote='"', escape='"', skip_blank_lines=True).load(f"{ttc_path}")
    ttc_path_df.createOrReplaceTempView("erf1_erf_cat_code_and_cat_item_code_4_paris_ttc_w_mapping_2_empower_cds")
    print(ttc_path_df.printSchema())
    # spark.sql("select * from erf1_erf_cat_code_and_cat_item_code_4_paris_ttc_w_mapping_2_empower_cds limit 30").show(truncate=0)
    
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
            print("failed recss after this")
            spark.sql('''select distinct s.CNTR_NUM,s.FUND_ID,s.CNTR_TYPE,
                        from srcdata s  where exists (select '1' from 
    				        tgtdata t 
    					where trim(t.CNTR_NUM) = trim(s.CNTR_NUM) 
    					and trim(t.FUND_ID) = trim(s.FUND_ID)
    					and trim(t.CNTR_TYPE) = trim(s.CNTR_TYPE)
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
            print(txt)
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



