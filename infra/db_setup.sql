-- Initialize database schema for credit card transactions
CREATE SCHEMA IF NOT EXISTS transactions;

CREATE TABLE IF NOT EXISTS transactions.credit_card_transactions (
 transaction_id SERIAL PRIMARY KEY, -- Auto-incrementing primary key
 transaction_date DATE NOT NULL,
 merchant VARCHAR(255) NOT NULL,
 category VARCHAR(255) NOT NULL,
 amount DECIMAL(10,2) NOT NULL,
 card_provider VARCHAR(255) NOT NULL
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_transaction_date 
    ON transactions.credit_card_transactions(transaction_date);

CREATE INDEX IF NOT EXISTS idx_merchant 
    ON transactions.credit_card_transactions(merchant);
