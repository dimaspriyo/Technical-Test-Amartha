# Case 2 Reconcile 

### Curl 
> curl --location 'localhost:3001' \  
--header 'Content-Type: application/json' \  
--data '{  
"transactions_path":"uploads/transactions/2025/05/15/transactions.csv",  
"bank_statements_path": [  
"uploads/bank_statements/2025/05/15/bank_A_statements.csv",  
"uploads/bank_statements/2025/05/15/bank_B_statements.csv"  
],  
"start_date": "01-05-2025",  
"end_date": "30-05-2025"  
}'


## Screenshoot
![Screenshoot](https://github.com/dimaspriyo/Technical-Test-Amartha/blob/main/Case-2/screenshot.png)
