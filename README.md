# Blog_Website
A blog website created in udemy course.
-- Updated SQL Query for PySpark SQL

WITH payment_with_funding AS (
    SELECT
        PayeeID,
        PaymentID,
        TransactionCategory AS SOURCE_TRANSACTION_CATEGORY,
        TransactionCode AS SOURCE_TRANSACTION_CODE,
        AccountFunding,
        SUBSTRING(AccountFunding, 1, 10) AS SOURCE_ACCT_CNTRCT_NUM,
        SUBSTRING(AccountFunding, 11, 6) AS SOURCE_SDIO,
        SUBSTRING(AccountFunding, LENGTH(AccountFunding) - 2, 3) AS SOURCE_TTC
    FROM
        payment
),
payment_with_deposit_date AS (
    SELECT
        p.PayeeID,
        p.PaymentID,
        p.SOURCE_TRANSACTION_CATEGORY,
        p.SOURCE_TRANSACTION_CODE,
        p.AccountFunding,
        p.SOURCE_ACCT_CNTRCT_NUM,
        p.SOURCE_SDIO,
        p.SOURCE_TTC,
        d.PaymentDate AS SOURCE_PAYMENT_DATE
    FROM
        payment_with_funding p
    LEFT JOIN
        deposit d
    ON
        p.PayeeID = d.PayeeID AND p.PaymentID = d.PaymentID
),
lookup_erf_cntrct AS (
    SELECT
        p.*,
        g.CNTRCT_NUM AS ERF_CNTRCT_NUM,
        g.CNTRCT_TYP_CD AS ERF_CNTRCT_TYP_CD,
        g.Fund_ID AS ERF_FUND_ID
    FROM
        payment_with_deposit_date p
    LEFT JOIN
        fund_loockup g
    ON
        p.SOURCE_SDIO = g.SDIO_ID
),
lookup_erf_trans_type AS (
    SELECT
        l.*,
        t.Trans_Typ_Cd AS ERF_TRANS_TYPE_CODE,
        t.Telus_Code AS ERF_ORIG_TTC
    FROM
        lookup_erf_cntrct l
    LEFT JOIN
        ttc_loockup t
    ON
        l.SOURCE_TTC = t.Telus_Code AND t.Telus_Primary_Ind = 'Y'
),
error_checks AS (
    SELECT
        *,
        CASE
            WHEN SOURCE_SDIO != 'PURANN' AND SUBSTRING(SOURCE_TRANSACTION_CODE, 9, 1) != 'P' THEN
                'Invalid SDIO and Transaction Code combination found in the source file'
            WHEN ERF_CNTRCT_NUM IS NULL THEN
                'Easy Group Account ID to ERF CNTRCT_NUM translation not found in the fund_loockup file'
            WHEN ERF_FUND_ID IS NULL THEN
                'SDIO ID to ERF Fund ID translation not found in the fund_loockup file'
            WHEN ERF_TRANS_TYPE_CODE IS NULL THEN
                'Telus Code to ERF Trans Type Code translation not found in the ttc_loockup file'
            ELSE NULL
        END AS ERROR_DESCRIPTION
    FROM
        lookup_erf_trans_type
)
SELECT
    'P_DB_PAYMENT_PAYMENTS_12062024_104719.csv' AS SOURCE_FILE_NAME,
    'DB_PAYMENT_DEPOSIT_12062024_104719.csv' AS OUTBOUND_FILE_NAME,
    'TELUS' AS SOURCE_SYSTEM,
    'ERF_DB_TRANS' AS TARGET_SYSTEM,
    SOURCE_ACCT_CNTRCT_NUM,
    NULL AS SOURCE_FUND_ID,
    SOURCE_SDIO,
    SOURCE_TTC,
    SOURCE_TRANSACTION_CATEGORY,
    SOURCE_TRANSACTION_CODE,
    SOURCE_PAYMENT_DATE,
    ERF_CNTRCT_NUM,
    ERF_CNTRCT_TYP_CD,
    ERF_FUND_ID,
    ERF_TRANS_TYPE_CODE,
    ERF_ORIG_TTC,
    NULL AS ERROR_CODE,
    ERROR_DESCRIPTION
FROM
    error_checks;



	=--------------------------
 -- SQL Query for PySpark SQL with table names updated

WITH payment_with_funding AS (
    SELECT
        PayeeID,
        PaymentID,
        TransactionCategory AS SOURCE_TRANSACTION_CATEGORY,
        TransactionCode AS SOURCE_TRANSACTION_CODE,
        AccountFunding,
        SUBSTRING(AccountFunding, 1, 10) AS SOURCE_ACCT_CNTRCT_NUM,
        SUBSTRING(AccountFunding, 11, 6) AS SOURCE_SDIO,
        SUBSTRING(AccountFunding, LENGTH(AccountFunding) - 2, 3) AS SOURCE_TTC
    FROM
        payment
),
payment_with_deposit_date AS (
    SELECT
        p.*,
        d.PaymentDate AS SOURCE_PAYMENT_DATE
    FROM
        payment_with_funding p
    LEFT JOIN
        deposit d
    ON
        p.PayeeID = d.PayeeID AND p.PaymentID = d.PaymentID
),
lookup_erf_cntrct AS (
    SELECT
        p.*,
        f.CNTRCT_NUM AS ERF_CNTRCT_NUM,
        f.CNTRCT_TYP_CD AS ERF_CNTRCT_TYP_CD,
        f.Fund_ID AS ERF_FUND_ID
    FROM
        payment_with_deposit_date p
    LEFT JOIN
        fund_loockup f
    ON
        p.SOURCE_SDIO = f.SDIO_ID
),
lookup_erf_trans_type AS (
    SELECT
        l.*,
        t.Trans_Typ_Cd AS ERF_TRANS_TYPE_CODE,
        t.Telus_Code AS ERF_ORIG_TTC
    FROM
        lookup_erf_cntrct l
    LEFT JOIN
        ttc_loockup t
    ON
        l.SOURCE_TTC = t.Telus_Code AND t.Telus_Primary_Ind = 'Y'
),
error_checks AS (
    SELECT
        *,
        CASE
            WHEN SOURCE_SDIO != 'PURANN' AND SUBSTRING(SOURCE_TRANSACTION_CODE, 9, 1) != 'P' THEN
                'Invalid SDIO and Transaction Code combination found in the source file'
            WHEN ERF_CNTRCT_NUM IS NULL THEN
                'Easy Group Account ID to ERF CNTRCT_NUM translation not found in the fund_loockup file'
            WHEN ERF_FUND_ID IS NULL THEN
                'SDIO ID to ERF Fund ID translation not found in the fund_loockup file'
            WHEN ERF_TRANS_TYPE_CODE IS NULL THEN
                'Telus Code to ERF Trans Type Code translation not found in the ttc_loockup file'
            ELSE NULL
        END AS ERROR_DESCRIPTION
    FROM
        lookup_erf_trans_type
)
SELECT
    'P_DB_PAYMENT_PAYMENTS_12062024_104719.csv' AS SOURCE_FILE_NAME,
    'DB_PAYMENT_DEPOSIT_12062024_104719.csv' AS OUTBOUND_FILE_NAME,
    'TELUS' AS SOURCE_SYSTEM,
    'ERF_DB_TRANS' AS TARGET_SYSTEM,
    SOURCE_ACCT_CNTRCT_NUM,
    NULL AS SOURCE_FUND_ID,
    SOURCE_SDIO,
    SOURCE_TTC,
    SOURCE_TRANSACTION_CATEGORY,
    SOURCE_TRANSACTION_CODE,
    SOURCE_PAYMENT_DATE,
    ERF_CNTRCT_NUM,
    ERF_CNTRCT_TYP_CD,
    ERF_FUND_ID,
    ERF_TRANS_TYPE_CODE,
    ERF_ORIG_TTC,
    NULL AS ERROR_CODE,
    ERROR_DESCRIPTION
FROM
    error_checks;

								
								
								
								

					
