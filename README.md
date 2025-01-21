            error_data.extend(spark.sql('''select distinct trim(s.SOURCE_ACCT_CNTRCT_NUM), trim(s.SOURCE_SDIO), trim(s.SOURCE_TTC), trim(s.SOURCE_TRANSACTION_CATEGORY), trim(s.SOURCE_TRANSACTION_CODE),
                    trim(s.ERF_CNTRCT_NUM), trim(s.ERF_CNTRCT_TYP_CD), trim(s.ERF_FUND_ID), trim(s.ERF_TRANS_TYPE_CODE), trim(s.ERROR_DESCRIPTION) from srcdata s
                    except
                    select distinct trim(t.SOURCE_ACCT_CNTRCT_NUM), trim(t.SOURCE_SDIO), trim(t.SOURCE_TTC), trim(t.SOURCE_TRANSACTION_CATEGORY), trim(t.SOURCE_TRANSACTION_CODE),
                    trim(t.ERF_CNTRCT_NUM), trim(t.ERF_CNTRCT_TYP_CD), trim(t.ERF_FUND_ID), trim(t.ERF_TRANS_TYPE_CODE), trim(t.ERROR_DESCRIPTION) from tgtdata t''').rdd.flatMap(lambda x:x).collect())
	    s3Client.put_object(Body='\n'.join(error_data), Bucket=tgt_bucket, Key=tgt_key+f'ERF9_Detail_Record_TestResult_Failures_{date_file}.TXT')
