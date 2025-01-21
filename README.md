 if (c == 'ERROR_DESCRIPTION'):
        
        test_case_number='31'
        
        # SOURCE_ACCT_CNTRCT_NUM|SOURCE_SDIO|SOURCE_TTC|SOURCE_TRANSACTION_CATEGORY|SOURCE_TRANSACTION_CODE|ERF_CNTRCT_NUM|ERF_CNTRCT_TYP_CD|ERF_FUND_ID|ERF_TRANS_TYPE_CODE|ERROR_DESCRIPTION
        
        print("Data Count Validation between source and Target")
        spark.sql('''select distinct trim(s.SOURCE_ACCT_CNTRCT_NUM), trim(s.SOURCE_SDIO), trim(s.SOURCE_TTC), trim(s.SOURCE_TRANSACTION_CATEGORY), trim(s.SOURCE_TRANSACTION_CODE),
                    trim(s.ERF_CNTRCT_NUM), trim(s.ERF_CNTRCT_TYP_CD), trim(s.ERF_FUND_ID), trim(s.ERF_TRANS_TYPE_CODE), trim(s.ERROR_DESCRIPTION) from srcdata s
                    except
                    select distinct trim(t.SOURCE_ACCT_CNTRCT_NUM), trim(t.SOURCE_SDIO), trim(t.SOURCE_TTC), trim(t.SOURCE_TRANSACTION_CATEGORY), trim(t.SOURCE_TRANSACTION_CODE),
                    trim(t.ERF_CNTRCT_NUM), trim(t.ERF_CNTRCT_TYP_CD), trim(t.ERF_FUND_ID), trim(t.ERF_TRANS_TYPE_CODE), trim(t.ERROR_DESCRIPTION) from tgtdata t  ''').show(truncate=0)
        
        test_case_count=str(spark.sql('''select count(*) from (select distinct trim(s.SOURCE_ACCT_CNTRCT_NUM), trim(s.SOURCE_SDIO), trim(s.SOURCE_TTC), trim(s.SOURCE_TRANSACTION_CATEGORY), trim(s.SOURCE_TRANSACTION_CODE),
                    trim(s.ERF_CNTRCT_NUM), trim(s.ERF_CNTRCT_TYP_CD), trim(s.ERF_FUND_ID), trim(s.ERF_TRANS_TYPE_CODE), trim(s.ERROR_DESCRIPTION) from srcdata s
                    except
                    select distinct trim(t.SOURCE_ACCT_CNTRCT_NUM), trim(t.SOURCE_SDIO), trim(t.SOURCE_TTC), trim(t.SOURCE_TRANSACTION_CATEGORY), trim(t.SOURCE_TRANSACTION_CODE),
                    trim(t.ERF_CNTRCT_NUM), trim(t.ERF_CNTRCT_TYP_CD), trim(t.ERF_FUND_ID), trim(t.ERF_TRANS_TYPE_CODE), trim(t.ERROR_DESCRIPTION) from tgtdata t ) ''').collect()[0][0])
        
        passed_recs=int(totalrecs)-int(test_case_count)
        
        print ('Test case number====' + str(test_case_number) + 'Failure record count====' + str(test_case_count))
        
        if (int(test_case_count)>0):
            teststatus = "Fail"
        else:
            teststatus = "Pass"
        
        if (int(test_case_count)>0):
            
            #print("source to target missing payeeid")
            
            spark.sql('''select distinct trim(s.SOURCE_ACCT_CNTRCT_NUM), trim(s.SOURCE_SDIO), trim(s.SOURCE_TTC), trim(s.SOURCE_TRANSACTION_CATEGORY), trim(s.SOURCE_TRANSACTION_CODE),
                    trim(s.ERF_CNTRCT_NUM), trim(s.ERF_CNTRCT_TYP_CD), trim(s.ERF_FUND_ID), trim(s.ERF_TRANS_TYPE_CODE), trim(s.ERROR_DESCRIPTION) from srcdata s
                    except
                    select distinct trim(t.SOURCE_ACCT_CNTRCT_NUM), trim(t.SOURCE_SDIO), trim(t.SOURCE_TTC), trim(t.SOURCE_TRANSACTION_CATEGORY), trim(t.SOURCE_TRANSACTION_CODE),
                    trim(t.ERF_CNTRCT_NUM), trim(t.ERF_CNTRCT_TYP_CD), trim(t.ERF_FUND_ID), trim(t.ERF_TRANS_TYPE_CODE), trim(t.ERROR_DESCRIPTION) from tgtdata t ''').show(truncate=0)
                    
            error_data.extend(spark.sql("select concat("+test_case_number+", '|',SOURCE_ACCT_CNTRCT_NUM, '|', SOURCE_SDIO, '|', SOURCE_TTC, '|', SOURCE_TRANSACTION_CODE,'|', ERROR_DESCRIPTION, '|', "+c+", '|', '"+c+"'                         ) as Res from tgtdata group by "+ c +" , SOURCE_ACCT_CNTRCT_NUM, SOURCE_SDIO, SOURCE_TTC,SOURCE_TRANSACTION_CODE, ERROR_DESCRIPTION having count(1) > 1 ".format(c)+ "limit 100").rdd.flatMap(lambda x:x).collect())
                
        txt = return_testcase_res(test_case_number,totalrecs,c,'85-100','NA',str(passed_recs),str(test_case_count),teststatus,'Data Count Validation between source and Target')
        
        fwf_data.append(txt)
        

# saving the test report ".txt" file to s3
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
        

        
