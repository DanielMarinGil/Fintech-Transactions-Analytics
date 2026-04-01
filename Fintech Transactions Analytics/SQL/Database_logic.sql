--Daniel Marin Gil
--Create a table for standardized financial reporting
CREATE TABLE Cleaned_transactions AS
WITH duplicated AS (
    --Identify and rank potential duplicate entries from the source system
    SELECT Tx_id, Customer_id, Transaction_date, Currency, Amount,
           ROW_NUMBER() OVER(PARTITION BY Tx_id ORDER BY Transaction_date) as rn
    FROM Transactions
),
joined_data AS (
    --Grouping for Forward Fill and reparing the dataset to handle missing FX rates
    SELECT 
        t.Tx_id,
        t.Customer_id,
        t.Transaction_date,
        t.Currency,
        t.Amount,
        f.Rate_to_EUR,
        --Creating dynamic groups to bring the last available rate
        COUNT(f.Rate_to_EUR) OVER (PARTITION BY t.Currency ORDER BY t.Transaction_date) as grp
    FROM duplicated t
    LEFT JOIN fx_rates f --Left join to keep those transactions that do not have a FX rate 
        ON t.Transaction_date = f.Rate_date 
        AND t.Currency = f.Currency
    WHERE t.rn = 1 --Only the unique record is kept to ensure data integrity
),
filled_fx AS (
    --Implementing a Forward Fill strategy for market gaps(weekends/holidays)for normalization
    SELECT 
        Tx_id,
        Customer_id,
        Transaction_date,
        Currency,
        Amount,
        --COALESCE ensures we recover the last non-null rate within each currency group
        COALESCE(Rate_to_EUR, FIRST_VALUE(Rate_to_EUR) OVER (
            PARTITION BY Currency, grp ORDER BY Transaction_date
        )) as Final_rate
    FROM joined_data
)
--Multi-currency valuation into a single base currency (EUR)
SELECT 
    Tx_id,
    Customer_id,
    Transaction_date,
    Currency,
    Amount,
    Final_rate,
    --Using 1 as default rate for EUR transactions
    CASE 
        WHEN Currency = 'EUR' THEN Amount 
        ELSE ROUND(Amount * Final_rate, 2) 
    END AS Amount_EUR
FROM filled_fx;
    Tx_id,
    Customer_id,
    Transaction_date,
    Currency,
    Amount,
    Final_rate,
    CASE 
        WHEN Currency = 'EUR' THEN Amount 
        ELSE ROUND(Amount * Final_rate, 2) 
    END as Amount_EUR
FROM filled_fx;