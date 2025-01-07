# Blog_Website
A blog website created in udemy course.
	Field Description	Conversion Center Field Names	"(I)n Progress, (C)omplete, (F)inal
(P)ending, (D)efer, (R)emove
(U)pdate, (N/A)
Wave1   |   Wave3   |   Wave4
"			"
Post 
Submit 
Change 
Date
"	Wave Comments	Field Type	Primary Key	Required (No means blank is valid)	Field Values	Notes	Field Information	Source File / Conversion Center	"Field / Table
Data Element"	Conversion Center Logic	Valid Values	"Conversion Center
Final Mapping"	"Wave 3 Mapping
Prudential"	"Wave 3 Mapping
Conversion"	Wave 4 Mapping	Comments

	SOURCE_FILE_NAME		P							Y				Conversion Center		File names of the Telus Payment & Deposit source files							SOURCE_SYSTEM		P							Y				Conversion Center		Set to "TELUS"							OUTBOUND_FILE_NAME		P							Y				Conversion Center		"Set to ""ERF_DB_TRANS_YYYYYMMDD_HHMMSS.TXT""
(date and time that the error report was created)"							TARGET_SYSTEM		P							Y						Set to "ERF"							SOURCE_ACCT_CNTRCT_NUM		P							Y				Payment File	AccountFunding	First 10 characters of AccountFunding							SOURCE_FUND_ID		P							N				Conversion Center		NULL							SOURCE_SDIO		P							Y				Payment File	AccountFunding	Next 6 characters of AccountFunding after the first 10							SOURCE_TTC		P							Y				Payment File	AccountFunding	Last 3 characters of AccountFunding							SOURCE_TRANSACTION_CATEGORY		P							Y				Payment File	TransactionCategory	TransactionCategory							SOURCE_TRANSACTION_CODE		P							Y				Payment File	TransactionCode	TransactionCode							SOURCE_PAYMENT_DATE		P					Date		Y	Convert to YYYY-MM-DD			"Payment

Deposit"	"
PaymentDate"	"Join the Payee ID and Payment ID in the Payment file to find the matching Payee ID and Payment ID row in the Deposit file in order to get the correct PaymentDate from the Deposit file

PaymentDate is what should be entered into the target file
(see image below for Deposit Record Specifications)"							ERF_CNTRCT_NUM		P											"Payment

erf_contracts w Fund ID with GG Lookup"	CNTRCT_NUM	"The data from the CNTRCT_NUM field is what should be entered into the target file

Only populate if an EASY_GROUP_ACCOUNT_ID to ERF CNTRCT_NUM match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"							ERF_CNTRCT_TYP_CD		P											"Payment

erf_contracts w Fund ID with GG Lookup"	CNTRCT_TYP_CD	"The data from the CNTRCT_TYP_CD field is what should be entered into the target file

Only populate if an SDIO_ID to ERF Fund ID match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"							ERF_FUND_ID		P											"Payment

erf_contracts w Fund ID GG Lookup"	"Fund ID
"	"The data from the Fund ID field is what should be entered into the target file

Only populate if an SDIO_ID to ERF Fund ID match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"							ERF_TRANS_TYPE_CODE		P											"Payment

ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes"	Trans Type Cd	"The data from Trans Type Cd field is what should be entered into the target file

Only populate if an Telus Code to ERF Trans Type Cd match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"							ERF_ORIG_TTC		P											"Payment

ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes"	Trans Type Cd	"Use the last 3 of the AccountFunding field in the TELUS Payment file to find the matching Trans Type CD in the ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes spreadhsheet by using TELUS Code and where Telus Primary Ind = ""Y"". 
If no match is found, do not send the row on the file.

Trans Type Cd is what should be entered into the target file
"							ERROR_CODE		P											Conversion Center		NULL							ERROR_DESCRIPTION		P							Y				Conversion Center		"See Description section (at the top) for additional information on requirements

One row per error found. 

List of Error Descriptions:
•	SDIO is not ""PURANN"" or 9th Character of the Transaction Code is not ""P"":
        o	     Error Description: ""Invalid SDIO and Transaction Code combination found in the source file""

For the following, evaluate rows that contain an ""A"" TransactionType, ""P"" in the 9th character of the TransactionCode, and ""PURANN"" as the SDIO (within the source file) and check for possible errors:

1•	No Translation found for Easy Group Account ID (GAID of AccountFunding) to ERF CNTRCT_NUM: 
        o	     Error Description: ""Easy Group Account ID to ERF CNTRCT_NUM translation not found in the erf_contracts w Fund ID with GG Lookup file""

2•	No Translation found for SDIO ID (SDIO of AccountFunding) to ERF Fund ID: 
        o	     Error Description: ""SDIO ID to ERF Fund ID translation not found in the erf_contracts w Fund ID with GG Lookup file""

3•	No Translation found for No Telus Code (TTC of AccountFunding) ERF to ERF Trans Type Code: 
        o	     Error Description: ""Telus Code to ERF Trans Type Dode translation not found in the ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes file""+Y24"			



















 								
								
Field Type	Primary Key	Required (No means blank is valid)	Field Values	Notes	Field Information	Source File / Conversion Center	"Field / Table
Data Element"	Conversion Center Logic
								
		N			"This is the column headers
(the 1st row in the target file)"	Conversion Center		"Only include header row if there is at least 1 error

SOURCE_SYSTEM,
OUTBOUND_FILE_NAME,
TARGET_SYSTEM,
SOURCE_ACCT_CNTRCT_NUM, 
SOURCE_FUND_ID, 
SOURCE_SDIO, 
SOURCE_TTC,
SOURCE_TRANSACTION_CATEGORY,
SOURCE_TRANSACTION_CODE,
ERF_CNTRCT_NUM,
ERF_CNTRCT_TYP_CD,
ERF_FUND_ID,
ERF_TRANS_TYPE_CODE,
ERF_ORIG_TTC,
ERROR_CODE,
ERROR_DESCRIPTION"
								
		Y				Conversion Center		File names of the Telus Payment & Deposit source files
		Y				Conversion Center		Set to "TELUS"
		Y				Conversion Center		"Set to ""ERF_DB_TRANS_YYYYYMMDD_HHMMSS.TXT""
(date and time that the error report was created)"
		Y						Set to "ERF"
		Y				Payment File	AccountFunding	First 10 characters of AccountFunding
		N				Conversion Center		NULL
		Y				Payment File	AccountFunding	Next 6 characters of AccountFunding after the first 10
		Y				Payment File	AccountFunding	Last 3 characters of AccountFunding
		Y				Payment File	TransactionCategory	TransactionCategory
		Y				Payment File	TransactionCode	TransactionCode
Date		Y	Convert to YYYY-MM-DD			"Payment

Deposit"	"
PaymentDate"	"Join the Payee ID and Payment ID in the Payment file to find the matching Payee ID and Payment ID row in the Deposit file in order to get the correct PaymentDate from the Deposit file

PaymentDate is what should be entered into the target file
(see image below for Deposit Record Specifications)"
						"Payment

erf_contracts w Fund ID with GG Lookup"	CNTRCT_NUM	"The data from the CNTRCT_NUM field is what should be entered into the target file

Only populate if an EASY_GROUP_ACCOUNT_ID to ERF CNTRCT_NUM match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"
						"Payment

erf_contracts w Fund ID with GG Lookup"	CNTRCT_TYP_CD	"The data from the CNTRCT_TYP_CD field is what should be entered into the target file

Only populate if an SDIO_ID to ERF Fund ID match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"
						"Payment

erf_contracts w Fund ID GG Lookup"	"Fund ID
"	"The data from the Fund ID field is what should be entered into the target file

Only populate if an SDIO_ID to ERF Fund ID match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"
						"Payment

ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes"	Trans Type Cd	"The data from Trans Type Cd field is what should be entered into the target file

Only populate if an Telus Code to ERF Trans Type Cd match was found via the mapping logic in the ERF-9 TELUS to ERF Tranx tab else NULL"
						"Payment

ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes"	Trans Type Cd	"Use the last 3 of the AccountFunding field in the TELUS Payment file to find the matching Trans Type CD in the ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes spreadhsheet by using TELUS Code and where Telus Primary Ind = ""Y"". 
If no match is found, do not send the row on the file.

Trans Type Cd is what should be entered into the target file
"
						Conversion Center		NULL
		Y				Conversion Center		"See Description section (at the top) for additional information on requirements

One row per error found. 

List of Error Descriptions:
•	SDIO is not ""PURANN"" or 9th Character of the Transaction Code is not ""P"":
        o	     Error Description: ""Invalid SDIO and Transaction Code combination found in the source file""

For the following, evaluate rows that contain an ""A"" TransactionType, ""P"" in the 9th character of the TransactionCode, and ""PURANN"" as the SDIO (within the source file) and check for possible errors:

1•	No Translation found for Easy Group Account ID (GAID of AccountFunding) to ERF CNTRCT_NUM: 
        o	     Error Description: ""Easy Group Account ID to ERF CNTRCT_NUM translation not found in the erf_contracts w Fund ID with GG Lookup file""

2•	No Translation found for SDIO ID (SDIO of AccountFunding) to ERF Fund ID: 
        o	     Error Description: ""SDIO ID to ERF Fund ID translation not found in the erf_contracts w Fund ID with GG Lookup file""

3•	No Translation found for No Telus Code (TTC of AccountFunding) ERF to ERF Trans Type Code: 
        o	     Error Description: ""Telus Code to ERF Trans Type Dode translation not found in the ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes file""+Y24"
								
								
								
								
								
								
								
								
								
								
								
								
								
								
								
								

					
