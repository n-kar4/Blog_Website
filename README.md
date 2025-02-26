df = spark.sql('''
    select 
        p.payeeid, 
        p.paymentid, 
        p.TransactionType
        p.TransactionCode as oldTransactionCode,
        concat(
            substr(p.TransactionCode, 1, 9), 
            pn.`Final Status`, 
            substr(p.TransactionCode, 11)
        ) as TransactionCode,
        p.AccountFunding,
        
        pp.taxID, 
        pn.SSN, 
        pn.master
    from payment p left join payee pp on p.payeeid = pp.payeeid 
    left join pn_excp pn on trim(pp.taxID) = trim(pn.SSN)
    where p.TransactionType!="D"
''')
