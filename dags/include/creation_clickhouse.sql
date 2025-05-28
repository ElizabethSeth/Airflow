DROP TABLE IF EXISTS card_data.bank_transactions;
CREATE TABLE IF NOT EXISTS card_data.bank_transactions
(
    Timestamp String,
    From_ID String,
    From_Account String,
    To_ID String,
    To_Account String,
    Amount Float64,
    Currency String,
    Converted_Amount Float64,
    Converted_Currency String,
    Transaction_Type String,
    Flag String,
    Suspicious_Type String
)
ENGINE = MergeTree
ORDER BY Timestamp;





