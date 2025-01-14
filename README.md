Description: This is the error log for the ERF-9 TELUS to ERF Tranx within this data contract.

Source Files: Telus Payment file (P_DB_PAYMENT_PAYMENTS_MMDDYYYY_HHMMSS.CSV), Telus Deposit file (P_DB_PAYMENT_DEPOSIT_MMDDYYYY_HHMMSS.CSV)

Logic to Exclude rows from the Telus Payment File:
1.	Filter out any row that does not contain an "A" TransactionType
2.	If the last letter in TransactionCode is "F", "G", or "H" (tax free payout codes), do not send these rows to ERF.


Error Log Requirements:
1.	One row per error found
2.	Only include a header if there is data. Otherwise, no. Send a blank file.
3.	If the 9th character of the TransactionCode within the Telus Payment file contains "P", but the SDIO is not "PURANN", error record and write to the error log
4.	If the 9th character of the TransactionCode within the Telus Payment file doesn not contain "P", but the SDIO is "PURANN", error record and write to the error log 
5.	If no Easy GAID (Easy GAID of AccountFunding) to ERF CNTRCT_NUM translation found, error record and write to the error log
6.	If no SDIO (SDIO of AccountFunding) to ERF Fund ID translation found, error record and write to the error log
7.	If no Telus Code (TTC of AccountFunding) to ERF Trans Type Code translation found, error record and write to the error log

List of Error Descriptions:
1.	SDIO is not "PURANN" or 9th Character of the Transaction Code is not "P":
        o	     Error Description: "Invalid SDIO and Transaction Code combination found in the source file"

For the following, evaluate rows that contain an "A" TransactionType, "P" in the 9th character of the TransactionCode, and "PURANN" as the SDIO (within the source file) and check for possible errors:

2.	No Translation found for Easy Group Account ID (GAID of AccountFunding) to ERF CNTRCT_NUM: 
        o	     Error Description: "Easy Group Account ID to ERF CNTRCT_NUM translation not found in the erf_contracts w Fund ID with GG Lookup file"
3.	No Translation found for SDIO ID (SDIO of AccountFunding) to ERF Fund ID: 
        o	     Error Description: "SDIO ID to ERF Fund ID translation not found in the erf_contracts w Fund ID with GG Lookup file"
4.	No Translation found for No Telus Code (TTC of AccountFunding) ERF to ERF Trans Type Code: 
        o	     Error Description: "Telus Code to ERF Trans Type Dode translation not found in the ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes file"

Target File Name Requirements:
•	ERF_DB_TRANS_ERROR_LOG

Target File Format Requirements:
•	Pipe delimited CSV file

Target File Location Requirements:
•	Place the error log of the weekly run in the erf/weekly folder
•	Place the error log of the monthly run in the erf/monthly folder












