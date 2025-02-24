# left join payee data with payment data on payeeid and show the table
df = spark.sql('''
    select 
        p.payeeid, 
        pp.payeeid, 
        p.paymentid, 
        p.TransactionCodeas as oldTransactionCode,
        concat(
            substr(p.TransactionCode, 1, 8), 
            pn.`Final Status`, 
            substr(p.TransactionCode, 10)
        ) as TransactionCode, 
        p.AccountFunding, 
        pp.taxID, 
        pn.SSN, 
        pn.master
    from payment p 
    left join payee pp on p.payeeid = pp.payeeid 
    left join PN_exception pn on trim(pp.taxID) = trim(pn.SSN)
''')

df.show(truncate=0)
print("data count", df.count())
