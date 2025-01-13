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
