NS:0  IP:0  Pnd:0  Defer:0  Comp:35  Rmv:0,,,,,,,,,,,,,,,,,,,,,,,,,,,,
ERF-1 PHX to ERF Transactions,Version Control,Field,"Description: Phoenix does not have the ability to distinguish what is/isn't an ERF fund, so only send rows where an ERF match is found. ERF-1 PHX to ERF is a daily transaction feed from Phoenix to ERF for ERF designated Funds Only.  Transactions include DB Expenses.  Feed should only contain Active Plans.  ERF is expecting Prudential legacy values (PRISM Contract Number, Investment Code, etc.). Phoenix will send 3 files (one at 1am, 4am, & 8am eastern). CaTs needs merge the files into one.",,,,,,,,,,,,,,,,,,,,,,,,,
Summary,,List,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,"Note: ERF-5 was removed from scope.  Today PARIS sends a separate file to ERF with the DB Expenses for Active Contracts. Joe discussed this the other day with Ramesh and they agree that Post TSA Phoenix will send all transactions including DB Expenses in the 3am and 5am files of ERF-1. CaTS will not try to split the DB Expenses into a separate file, ERF was going to make a change to their scripting so they don't expect a separate file.",,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,"Source File: XF0000TR_YYYYMMDD_HHMMSS.FIL (aka Extract 3, Transaction History) / Target file: ERF_DB_TRANS",,,,,,,,,,,,,,,,,,,,,,,,,
Field Description,Conversion Center Field Names,"(I)n Progress, (C)omplete, (F)inal",,,,Wave Comments,Field Type,Primary Key,Required (No means blank is valid),Field Values,Start ,End,Length,Default Value in EASY,JSON,Notes,Field Information,Source File / Conversion Center,Field / Table,Start Position,End Position,Conversion Center Logic,Valid Values,Conversion Center,Wave 3 Mapping,Wave 3 Mapping,Wave 4 Mapping,Comments
,,"(P)ending, (D)efer, (R)emove",,,Post,,,,,,,,,,,,,,Data Element,,,,,Final Mapping,Prudential,Conversion,,
,,"(U)pdate, (N/A)",,,Submit,,,,,,,,,,,,,,,,,,,,,,,
,,Wave5   |   Wave3   |   Wave4,,,Change,,,,,,,,,,,,,,,,,,,,,,,
,,,,,Date,,,,,,,,,,,,,,,,,,,,,,,
xx Header Record,,F,,,,,,,,,,,,,,,,,,,,,,,,,,
xx NONE,,F,,,,,,,,,,,,,,,,,,,,,,,,,,
xx Detail Record,,F,,,,,,,,,,,,,,,,,,,,,,,,,,
PARIS-REC-TYPE,,U,,,45376,,XX,,Y,Fees = 02,1,2,2,,,Fees are not traded transactions.  Look at ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes spreadhsheet.  Look at Erf Ctgy Cd for Expenses; Expenses would be Record 2 else Record Type 1.,"HD = header, TR = Trailer, 02 = Detail Record - check with paris on 01, 04, 05 etc - sent email 3/7 to Paris","Extract, 3 Transaction History",Phoenix Code (column L),,,"Use Extract 3, Transaction History - Transaction Type (46-48) to match to ERF CD&Item CD:Phoenix Code Selected (column N) on ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes.xlsx return ERF CD&Item Cd.Descriptions (column O) - If Descriptions (column O) = Variable Fee Paid, Return '02' else Return '01'",,,,,,
,,,,,,,,,,Else 01,,,,,,,,ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes (Tab ERF CD&Item Cd),Phoenix Description (column O),,,,,,,,,
CNTR-NUM,,F,,,,,X(6),,Y,,3,8,6,,,Prudential Contract # the first 6 of the AFR,New DB Plan - Manual process in ERF to setup a new contract # - Will maintain table in CaTS with new GAID and ERF Plan IDs - Who will maintain this table in CaTS - Eunice will take back to CC,"Extract, 3 Transaction History",Shareholder Account Number,11,19,"Use Extract 3, Transaction History; Shareholder Account Number to lookup EASY Group Account Id (Column L); return CNTRCT _NUM (Column B) on erf_contracts w Fund ID with GG Lookup.xlsx. Only send rows where there's a match in the erf_Contracts w Fund ID with GG Lookup",,,,,,
,,,,,,,,,,,,,,,,,,erf_Contracts w Fund ID with GG Lookup,,,,,,,,,,
CNTR-TYPE,,F,,,,,XXX,,Y,,9,11,3,,,Check DB Data Contract - ,"Randall has the file with the Contract Type - Will email - It is part of the AFR (7,8,9).  What will be the EASY equivelant for new DB Plans","Extract, 3 Transaction History",Shareholder Account Number,11,19,"Use Extract 3, Transaction History; Shareholder Account Number to lookup EASY Group Account Id (Column L); return CNTRCT _TYP_CD (Column C) on erf_contracts w Fund ID with GG Lookup.xlsx.  Only send rows where there's a match in the erf_Contracts w Fund ID with GG Lookup",,,,,,
,,,,,,,,,,,,,,,,,,erf_Contracts w Fund ID with GG Lookup,,,,,,,,,,
FUND-ID,,F,,,,,XXX,,Y,,12,14,3,,,,Will be maintained in CaTS who will be responsible for maintianing CaTS ,"Extract, 3 Transaction History",Fund Number,1,10,"Use Extract 3, Transaction History; Fund Number to lookup SDIO ID (Column N); return Fund ID (Updated) (Column E) on erf_contracts w Fund ID with GG Lookup.xlsx.  Only send rows where there's a match in the erf_Contracts w Fund ID with GG Lookup",,,,,,
,,,,,,,,,,,,,,,,,,erf_Contracts w Fund ID with GG Lookup,,,,,,,,,,
CNTR-SUB,,F,,,45418,,XXX,,,,15,17,3,,,,Part of the AFR - Is this the same concept of Divisions in TELUS.  How will all the new ABC Divisions be handled in ERF,"Extract, 3 Transaction History",Shareholder Account Number,11,19,"Use Extract 3, Transaction History; Shareholder Account Number to lookup EASY Group Account Id (Column L); return PRU Contract SUB (Column K) on erf_contracts w Fund ID with GG Lookup.xlsx. If no match send Null",,,,,,
,,,,,,,,,,,,,,,,,,erf_Contracts w Fund ID with GG Lookup,,,,,,,,,,
PREM-TAX,,F,,,,,X,,,,18,18,1,,,,,Conversion Center,,,,space(1),,,,,,
TB,,F,,,,,XX,,,,19,20,2,,,,,Conversion Center,,,,spaces(2),,,,,,
TCN,,F,,,,,X(10),,,,21,30,10,,,,,Conversion Center,,,,spaces(10),,,,,,
ORIG-TTC,,U,,,45384,,X(5),,Y,,31,35,5,,,,,"Extract, 3 Transaction History",Transaction Type,46,48,"Use Extract 3, Transaction History; Transaction Type to lookup Phoenix Code (Column K); return Trans Typ CD (Column A) on ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes.xlsx Tab ERF CD&Item Cd. Only send rows where there's a match in the ERF Cat Code Cat Item Code Lookup (longer name listed)",,,,,,
,,,,,,,,,,,,,,,,,,ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes (Tab ERF CD&Item Cd),Novation Ind (column H),,,,,,,,,
SPEC-TTC,,F,,,,,X(5),,"Yes, based on Paris-Rec-Type",,36,40,5,,,"SPEC-TTC is populated for record types '02' and '03'. The unique SPEC-TTC's for these records are ME401,402,406 & MA400,402,403 going by the last months history (from Sai 9/6/23) - breakage / slippage / backdated transactions for Separate Accounts only not General Account funds (D.Katz on call 9/8)",Used for allocation of Fee Income \ Breakage - If there was a backdated transaction this is the slippage TTC.  This is not used for the General Account,Conversion Center,,,,If PARIS-REC-TYPE is '02' then SPEC-TTC is populated for record types '02',,,,,,
,,,,,,,,,,,,,,,,,,,,,,else spaces(5),,,,,,
FUND-EFF,,F,,,,,X,,,,41,41,1,,,,,Conversion Center,,,,space(1),,,,,,
ASSET-EFF,,F,,,,,X,,,,42,42,1,,,,,Conversion Center,,,,space(1),,,,,,
CNTR-DT,,F,,,,,X(10),,Y,yyyy-mm-dd,43,52,10,,,,,"Extract, 3 Transaction History",Date Of Trade,33,40,Format CCYYMMDD to yyyy-mm-dd,,,,,,
VCH-DT,,U,,,45667,,X(10),,Y,yyyy-mm-dd,53,62,10,,,,"The Date that the Expense was processed in EASY what will we use from TELUS for Benefit Payments (Phoenix will have transactions directly added (Employer Contributions, Withdrawals, Adjustments etc.)","Extract, 3 Transaction History",Process Date,219,226,"If the date in the file name falls on a Monday, Voucher Date (VCH-DT) = date in file name - 3 days (verify that if Friday is a Holiday, subtract one more day.)",,,,,,
,,,,,,,,,,,,,,,,,,EASY_Bus_Calendar.csv,,,,"Else Voucher Date (VCH-DT) = date in file name - 1 day (verify that if date is a Holiday, subtract one more day.)",,,,,,
,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,,,Format CCYYMMDD to yyyy-mm-dd,,,,,,
SOURCE-SYSTEM,,F,,,45418,,X(6),,,,63,68,6,,,,,Conversion Center,,,,spaces(6),,,,,,
GEN-YEAR,,F,,,,,X(4),,,,69,72,4,,,,,Conversion Center,,,,spaces(4),,,,,,
GEN-PERIOD,,F,,,,,X,,,,73,73,1,,,,,Conversion Center,,,,spaces(1),,,,,,
BUS-TYPE,,F,,,,,X(6),,,,74,79,6,,,,,Conversion Center,,,,spaces(6),,,,,,
SRVC-AGREE,,U,,,45401,,XXXXX,,,,80,84,5,,,,,Conversion Center,,,,spaces(5),,,,,,
TRANS-AMOUNT,,U,,,45442,,S9(12).99,,Y,ex: +999999999999.99 or -99999999999.99,85,100,16,,,,,"Extract, 3 Transaction History",Dollar Amount,101,119,Remove 3 leading 0s to match length of ERF field length of 16.  Leave '-' for negative values.,,,,,,
,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,,,"If Reversal Flag on Extract 3, Transaction History equals '1', TRANS-AMOUNT should be multiplied by -1",,,,,,
,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,,,. Skip record if there is no amount/zero amount,,,,,,
LEGACY-CNTR,,F,,,,,X(7),,,,101,107,7,,,,,Conversion Center,,,,spaces(7),,,,,,
LEGACY-SUB-FUND,,F,,,,,XX,,,,108,109,2,,,,,Conversion Center,,,,spaces(2),,,,,,
LEGACY-FUND,,F,,,,,X(7),,,,110,116,7,,,,,Conversion Center,,,,spaces(7),,,,,,
NESTED-FUND-ID,,F,,,,,XXX,,,,117,119,3,,,,,Conversion Center,,,,spaces(3),,,,,,
DTR-TTC,,U,,,45384,,X(5),,Y,,120,124,5,,,"Logic around DTR-TTC field from DTR standpoint:- For record types '01' , DTR-TTC is used if it is valued, else ORIG-TTC is used (from Sai 9/6/23) - same as orig-ttc, no exceptions for general account so this would never be any different than orig-ttc (D.Katz 9/8 meeting)",,"Extract, 3 Transaction History",Transaction Type,46,48,"Use Extract 3, Transaction History; Transaction Type to lookup Phoenix Code (Column K); return Trans Typ CD (Column A) on ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes.xlsx Tab ERF CD&Item Cd. Only send rows where there's a match",,,,,,
,,,,,,,,,,,,,,,,,,ERF Cat Code and Cat Item Code for PARIS TTC with mapping to Empower Codes (Tab ERF CD&Item Cd),Novation Ind (column H),,,,,,,,,
FILLER,,F,,,,,X(76),,,,125,200,76,,,,,Conversion Center,,,,spaces(76),,,,,,
xx Trailer Record,,F,,,,,,,,,,,,,,,,,,,,,,,,,,
WS-TRL-REC-TYPE,,F,,,,,XX,,,,1,2,2,,,,,Conversion Center,,,,TR,,,,,,
WS-TRL-ROW-COUNT,,F,,,,,9(6),,Y,Default with zeroes ex:000012,3,8,6,,,,,Conversion Center,,,,Count [Detail Records],,,,,,
WS-TRL-HASH-TOTAL,,F,,,,,9(16),,,,9,24,16,,,,,Conversion Center,,,,spaces(16),,,,,,
WS-TRL-TOT-TRANS-AMT,,U,,,45398,,S9(15).99,,,ex: +999999999999999.99 or -999999999999999.99,25,43,19,,,,,Conversion Center,TRANS-AMOUNT,,,Sum TRANS-AMOUNT(85-100),,,,,,
FILLER,,F,,,,,X(157),,,,44,200,157,,,,,Conversion Center,,,,space(157),,,,,,
,,,,,,,,,,,,,,,,,,,,,,,,,,,,
,,,,,,,,,,,,,,,,,,,,,,,,,,,,
CaTS File Feed Requirements,,,,,,,,,,,,,,,,,,,,,,,,,,,,
File Feed Name,Conversion Center to Define Do not want Date or Time Stamp in File name,,,,,,,,,,,,,,,,,,,,,,,,,,,
Description ,"ERF-1 PHX to ERF is a daily transaction feed from Phoenix to ERF for ERF designated Funds Only.  Transactions include DB Expenses.  Feed should only contain Active Plans.  ERF is expecting Prudential legacy values (PRISM Contract Number, Investment Code, etc.)",,,,,,,,,,,,,,,,,,,,,,,,,,,
File Format,Fixed Length,,,,,,,,,,,,,,,,,,,,,,,,,,,
Frequency,Daily,,,,,,,,,,,,,,,,,,,,,,,,,,,
Time or Day Needed,ERF Processing is done at 7am would want file no later then that,,,,,,,,,,,,,,,,,,,,,,,,,,,
"Define Monthly, QTR or Annual",N/A,,,,,,,,,,,,,,,,,,,,,,,,,,,
Data Extract Logic,ERF Funds Only - Identified by using the list of Funds on the erf_contracts w Fund ID with GG Lookup.xlsx,,,,,,,,,,,,,,,,,,,,,,,,,,,
Sorting or Grouping  Needed,,,,,,,,,,,,,,,,,,Aggregate by ERF-1 Field Names: CNTR_NUM; VCH-DT; FUND-ID; CNTR-SUB; ORIG-TTC,,,,,,,,,,
,,,,,,,,,,,,,,,,,,SUM : TRANS_AMOUNT,,,,,,,,,,
Merge Files,,,,,,,,,,,,,,,,,,,,,,,,,,,,
Source File,"Extract, 3 Transaction History",,,,,,,,,,,,,,,,,,,,,,,,,,,
CaTS Mapping Spreadsheets,1) erf_contracts w Fund ID with GG Lookup.xlsx,,,,,,,,,,,,,,,,,,,,,,,,,,,
,2) TTC Phoenix Codes Mapping_.xlsx,,,,,,,,,,,,,,,,,,,,,,,,,,,
Will you be able to isolate invalid records or will you need a new full file?,,,,,,,,,,,,,,,,,,,,,,,,,,,,
Who will Receive the a list of the Invalid Records,Production Support Team ongoing discussion with CaTS,,,,,,,,,,,,,,,,,,,,,,,,,,,











CALENDAR_DATE,TYPE_DATE,TYPE_DESCRIPTION                                       
1-Jan-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
2-Jan-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Jan-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Jan-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Jan-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Jan-24,CBOQ      ,3RD FRIDAY OF MONTH FOLLOWING QUARTER END.                  
          ,CBUQ      ,QUARTERLY COMMISSION CALENDAR - 3rd BUSINESS DAYS AFTER QE  
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Jan-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
16-Jan-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Jan-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Jan-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Jan-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Jan-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Feb-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Feb-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
4-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
5-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Feb-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
11-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
12-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Feb-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
18-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
19-Feb-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
20-Feb-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Feb-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
25-Feb-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
26-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Feb-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Feb-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Mar-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
3-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
4-Mar-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Mar-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
10-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
11-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Mar-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
17-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
18-Mar-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Mar-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
24-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
25-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Mar-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Mar-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,HOQE      ,HOME OFFICE QUARTER END - LAST BUS. DAY OF QTR              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Mar-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
30-Mar-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
31-Mar-24,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   
          ,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
1-Apr-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Apr-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Apr-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Apr-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,CBOQ      ,3RD FRIDAY OF MONTH FOLLOWING QUARTER END.                  
          ,CBUQ      ,QUARTERLY COMMISSION CALENDAR - 3rd BUSINESS DAYS AFTER QE  
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Apr-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Apr-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Apr-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Apr-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Apr-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Apr-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-May-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-May-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
5-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
6-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-May-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-May-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
12-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
13-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
19-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
20-May-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-May-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-May-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
26-May-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
27-May-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
28-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-May-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-May-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
2-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
3-Jun-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Jun-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Jun-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
9-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
10-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
16-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
17-Jun-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Jun-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
20-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Jun-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
23-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
24-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Jun-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Jun-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,HOQE      ,HOME OFFICE QUARTER END - LAST BUS. DAY OF QTR              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Jun-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
30-Jun-24,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   
          ,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
1-Jul-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Jul-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Jul-24,CBUQ      ,QUARTERLY COMMISSION CALENDAR - 3rd BUSINESS DAYS AFTER QE  
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Jul-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
5-Jul-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Jul-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Jul-24,CBOQ      ,3RD FRIDAY OF MONTH FOLLOWING QUARTER END.                  
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Jul-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Jul-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Jul-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Jul-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Jul-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Jul-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Aug-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Aug-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
4-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
5-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Aug-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
11-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
12-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Aug-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
18-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
19-Aug-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Aug-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
25-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
26-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Aug-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Aug-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Aug-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
1-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
2-Sep-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
3-Sep-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Sep-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
9-Sep-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Sep-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
16-Sep-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Sep-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
23-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Sep-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Sep-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Sep-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
30-Sep-24,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   
          ,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,HOQE      ,HOME OFFICE QUARTER END - LAST BUS. DAY OF QTR              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Oct-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Oct-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Oct-24,CBUQ      ,QUARTERLY COMMISSION CALENDAR - 3rd BUSINESS DAYS AFTER QE  
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
6-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Oct-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Oct-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
13-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Oct-24,CBOQ      ,3RD FRIDAY OF MONTH FOLLOWING QUARTER END.                  
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
20-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Oct-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Oct-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Oct-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
27-Oct-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Oct-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Oct-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Nov-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
3-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
4-Nov-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Nov-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Nov-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
10-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
11-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
17-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
18-Nov-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Nov-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Nov-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
24-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
25-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Nov-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Nov-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
29-Nov-24,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Nov-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
1-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
2-Dec-24,CHBK      ,COMMISSIONS CHARGEBACK -TERMINATING PER MARS                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Dec-24,ABOM      ,MONTH END DATE FOR PROCESSING ASSET COMP                    
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Dec-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
9-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
16-Dec-24,CBOM      ,COMMISSION CUTOFF BEFORE BOME (DATES PROVIDED BY LLIJ)      
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Dec-24,BOME      ,BRANCH OFFICE MONTH END - DATES PROVIDED BY CORP COMM ADMIN 
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Dec-24,BWKF      ,BI-WEEKLY FRIDAY PCGR PROCESSING                            
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
23-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Dec-24,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
26-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Dec-24,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
30-Dec-24,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Dec-24,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   
          ,HOME      ,HOME OFFICE MONTH END - LAST BUS. DAY OF MONTH              
          ,HOQE      ,HOME OFFICE QUARTER END - LAST BUS. DAY OF QTR              
          ,HOYE      ,HOME OFFICE YEAR END - LAST BUS. DAY OF YEAR                
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Jan-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
2-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
5-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
6-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
12-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
13-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
19-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
20-Jan-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
21-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Jan-25,FINH      ,GWL HOME OFFICE WORK DAY                                    
25-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
26-Jan-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
27-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Jan-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
2-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
3-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
9-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
10-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
16-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
17-Feb-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
18-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
23-Feb-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
24-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Feb-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
2-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
3-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
9-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
10-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
16-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
17-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
23-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
24-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Mar-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
30-Mar-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
31-Mar-25,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
6-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
13-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Apr-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
19-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
20-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
27-Apr-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Apr-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
4-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
5-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
11-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
12-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
18-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
19-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
25-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
26-May-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
27-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-May-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-May-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
1-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
2-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
9-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
16-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Jun-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
20-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
23-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Jun-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Jun-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
30-Jun-25,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Jul-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
5-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
6-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
13-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
20-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
27-Jul-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Jul-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
3-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
4-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
10-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
11-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
17-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
18-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
24-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
25-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Aug-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
31-Aug-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
1-Sep-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
2-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Sep-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Sep-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Sep-25,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   
          ,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
5-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
6-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
12-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
13-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
19-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
20-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
26-Oct-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
27-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
28-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Oct-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
1-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
2-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
3-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
7-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
8-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
9-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
10-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
14-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
15-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
16-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
17-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
21-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
22-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
23-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
24-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
26-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Nov-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
28-Nov-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
29-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
30-Nov-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
1-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
2-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
3-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
4-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
5-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
6-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
7-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
8-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
9-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
10-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
11-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
12-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
13-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
14-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
15-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
16-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
17-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
18-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
19-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
20-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
21-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
22-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
23-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
24-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
25-Dec-25,FINH      ,FINANCIAL HOLIDAY - STOCK MARKET CLOSED DAY                 
26-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
27-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
28-Dec-25,WEND      ,WEEKEND DAY - NOT ON AUTOSYS                                
29-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
30-Dec-25,WORK      ,GWL HOME OFFICE WORK DAY                                    
31-Dec-25,CAQE      ,CALENDAR QUARTER END (LAST DAY OF MONTH) - NOT ON AUTOSYS   

