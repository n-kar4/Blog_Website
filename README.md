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
								

	----------------------------------------------------------------------------------------------------------------

payment table
 PayeeID	PaymentID	TransactionCategory	TransactionType	TransactionCode	AccountFunding	TransactionAmount_Previous	TransactionAmount	TransactionPercent
10000000007Z17409P01	8859e030-177f-4077-afaa-c83c42c3d8b0	EA-LS-TRAN	A	Z17409ZQNNTT0694R	556407-E1.Z678PD.E01		65628.9	
10000000031Z00940P01	82ea68b4-221c-4319-88f1-c4f17df8da55	RETRO	A	Z00940ZQPNMP0861P	523662-E1.PURANN.E01	0	3384.16	
10000000068Z17051P01	c48cd381-d970-41aa-bee7-ee9e1201cb88	EA-LS-TRAN	A	Z17051ZQNNTT0007R	194540-E1.J060PD.E01		52483.02	
10000000153D17349P01	f451b593-ce85-4d91-a7ac-f1f4af831905	EA-LS-TRAN	A	D17349ZQNNTT0667R	556245-E5.Z346PD.E01		1405124.28	

----------------------------------------------------------
deposit table
PayeeID	PaymentID	PaymentDate	PaymentFrequency	RoutingNumber	AccountType	BankAccount	PayableTo	FBO
10000000360Z17470P01	060736b9-14e7-429f-b9cf-04221e7ef098	12/13/2024	M	314074269	22	186995725		
10000008407Z16741P01	971b8160-7856-43b9-9a52-eb5f4f300420	12/13/2024					ANTONIO CUADROS	
10000061334Z17411B01	4281367e-84b2-4b15-a6bd-2cb723ee20f4	12/13/2024					CHARLES SCHWAB & CO INC	* 20907879 JENAYA L SAGE-CHAVEZ
10000123070Z17284P02	40e44323-081e-4d7c-9bed-96a39173f3e3	12/13/2024	M	64000059	22	292487949		

--------------------------------------------------------------------------
ggloockup
CNTRCT_NO	CNTRCT_NUM	PRU_Contract_SUB	CNTRCT_TYP_CD	ERF_STAT_CD	Fund_ID	Series_Id	EASY_Group_Account_Id	EASY_Group_Client_Id	SDIO_ID	Pru_Client_Name	Pru_Plan_Name	Action_based_on_Randy's_email	Reasoning	Dup_check
65	65	1	F21	D       	6C1		523352-E1	523352	PURANN	ARMSTRONG WORLD INDUSTRIES	Armstrong Cork Company			
67	67	1	F11	D       	6C1		523630-E1	523630	PURANN	OAKWOOD SCHOOL INC	Oakwood School Inc			
86	86	1	F11	D       	6C1		150002-E2	150002	PURANN	NAVISTAR INTERNATIONAL CORP	International Truck & Engine Corporation			
102	102	1	F21	D       	6C1		523652-E1	523652	PURANN	JOHNSON CONTROLS WORLD SERVICE	Pan American Airways Incorporated			
105	105	1	F21	A       	6C1		556411-E1	556411	PURANN	Raytheon Technologies	United Technologies Corporation			
105	105	101	F21	A       	6C1		556411-E5	556411	PURANN	Raytheon Technologies	United Technologies Corporation	Change	Deal with A CT	

------------------------------------------------------------------
ttcloockup
Trans_Typ_Cd	Erf_Ctgy_Cd	Erf_Ctgy_Item_Cd	Row_Eff_Dt	Row_End_Dt	ERF_Primary_Ind	EASY_Primary_Ind	EASY_FAT_Codes	EASY_FAT_Description	Phoenix_Primary_Ind	Phoenix_Code	Phoenix_Description	Telus_Primary_Ind	Telus_Code
IC073	CONTRIBS	ERCONTRB	1/1/2001	12/31/9999			CNT	0	Y	100	Employer Contributions		E34
IC349	CONTRIBS	LNRPYPRN	1/1/2001	12/31/9999	Y		0	LOAN REPAYMENT		101	Variable Transfer		N/A
IC352	CONTRIBS	LNRPYINT	1/1/2001	12/31/9999	Y		0	LOAN REPAYMENT	Y	101	Variable Transfer		N/A
TR080	TRNSFRSA	OUTFNDTX	1/1/2007	12/31/9999	Y		TIE	TRANSFER IN		102	Plan Transfer In		N/A

								
								
								
								
								
								
								
								

					
