Hi Nilanjan,
 
It is 4 step process before categorizing a row from payment file.
 
1) Adjust the Novation Indicator for specific individuals

The code extracts the Plan from the PayeeID by taking characters from index 11 to 17.

Iterates through each row of the payment file, and itt Skips rows with TransactionType "D".  

If the TransactionType for the current row is "D", the loop skips to the next iteration.
 
Filters the novation_exceptions DataFrame to find rows where the 

	- SSN matches the TaxID and 

	- The PAYEEID MASTER CONTRACT matches the Plan for the current row in the Payment. 

If there is more than one match, an error message is printed.

If there is exactly one match:

	-The TransactionCode for the current row is updated 

	-By replacing the 10th character with the Final Status from the match. 

	-The Adjustment column is set to "Yes"

	-The Adjustment_Info column is updated with details about the change, 

	including the original and new TransactionCode, SSN, PAYEEID MASTER CONTRACT, TaxID, and Plan.
 
This block of code adjusts the TransactionCode for specific individuals based on novation exceptions.
 
========================================================================================

2) Adjust the G/N/P indicator to "P" for individuals with the PURANN SDIO
 
Filtering Rows: Filters rows where the Plan is in gnp_changes and AccountFunding is not null.
 
Checks if the substring between the first and second dots is PURANN.

If the AccountFunding string contains "PURANN", the TransactionCode for the current row is updated by 

	-Replacing the 9th character with "P". 

	-The Adjustment column is set to "Yes", and 

	-The Adjustment_Info column is updated with details about the change, 

	-including the original and new TransactionCode, AccountFunding, and Plan.
 
This block of code ensures that the G/N/P indicator is correctly adjusted to "P" for individuals with the PURANN SDIO.
 
===============================================================================================

3) Adjust G/N/P indicator for certain disability payments and adjust SDIO and G/N/P indicator for certain non-disability payments
 
Filters rows where the Plan is in the keys of sdio_changes.
 
It then iterates through the indices of these filtered rows.
 
Checks if the GAID in AccountFunding matches the necessary GAID from sdio_changes.
 
 
Updating TransactionCode for Disability Payments: 

	- Updates the 9th character of the TransactionCode to "N" if the last character is "D",  it indicates a disability payment.

	- Sets the Adjustment column to "Yes" and updates the Adjustment_Info column with details of the change.
 
Updating AccountFunding and TransactionCode for Non-Disability Payments: 

	- If the TransactionCode does not end with "D", it indicates a non-disability payment. 

	- Updates the AccountFunding to include PURANN and sets the 9th character of TransactionCode to "P".
 
This block of code ensures that the G/N/P indicator is correctly adjusted for certain disability payments and that the SDIO and G/N/P indicator are adjusted for certain non-disability payments based on the provided sdio_changes.
 
=================================================================================================
 
4) Replace temporary SDIOs with PURANN and change G/N/P indicator of 1 plan
 
Iterating Over Rows: Iterates over each row in the raw_payment DataFrame.
 
Verifies if the SDIO is PURANN. If the substring between the first and second dots in the AccountFunding string is "PURANN", 

	- the TransactionCode for the current row is updated by replacing the 9th character with "P".
 
 
 
If the substring between the first and second dots in the AccountFunding string is in the sdio_replace list, 

the AccountFunding string is updated by replacing the substring between the first and second dots with "PURANN". 

The TransactionCode is updated by replacing the 9th character with "P".
 
 
If the Plan is in non_guaranteed and the AccountFunding matches the GAID and SDIO:

Updates the 9th character of the TransactionCode to "N"
 
The sdio_replace list contains SDIO values that need to be replaced with "PURANN".

These values are read from a CSV file located at CHANGES_INPUT_PATH.

The CSV file is read into a DataFrame called changes.

The DataFrame is filtered to include only rows where the Change column is "PURANN".

The SDIO column from this filtered DataFrame is converted to a list and assigned to sdio_replace
 
