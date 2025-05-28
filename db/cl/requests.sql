create table if not exists card_data.transactions (Timestamp String,From_ID String,To_ID String,To_Account String,Amount String,Currency String,Converted_Amount String,Converted_Currency String,Transaction_Type String,Flag String,Suspicious_Type String) ENGINE=MergeTree() order by Timestamp;
select * from card_data.transactions
limit 100;