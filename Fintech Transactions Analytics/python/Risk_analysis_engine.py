import sqlite3
import pandas as pd
from datetime import datetime

#Database Connection
#We use sqlite3 to keep the project self-contained and easy to replicate. SQLite was used.
connect_sql = sqlite3.connect('fX_data.db')

#We pull from a Cleaned View to ensure we work with de-duplicated data from the start
query = "SELECT * FROM Cleaned_transactions"
df = pd.read_sql_query(query, connect_sql)

# Standardizing to datetime to allow for precise recency calculations
df["Transaction_date"] = pd.to_datetime(df["Transaction_date"])

# We group by ID and Name to preserve customer identity throughout the analysis, .agg() method from pandas to perform calculations
client_metrics = df.groupby(["Customer_id", "Customer_name"]).agg(
    total_spent_eur=("Amount_EUR", "sum"),
    avg_ticket_eur=("Amount_EUR", "mean"),
    last_transaction=("Transaction_date", "max"),
    transaction_count=("Tx_id", "count")
).reset_index()

#We fill NaNs with 0 to ensure our 100% customer coverage including inactive leads
client_metrics["total_spent_eur"] = client_metrics["total_spent_eur"].fillna(0).round(2)
client_metrics["transaction_count"] = client_metrics["transaction_count"].fillna(0).astype(int)
client_metrics["avg_ticket_eur"] = client_metrics["avg_ticket_eur"].fillna(0).round(2)

# Using the max date in the dataset as "Today" to simulate a real-time snapshot
latest_date = df["Transaction_date"].max()

#Segmentation Logic
#We calculate the 80th percentile just for active spenders. Pareto principal approach a minority of clients can generate a big part of revenue
#This prevents client that spent 0 from lowering our "Top Tier" threshold avoiding statistical skew.
active_clients = client_metrics[client_metrics["total_spent_eur"] > 0]
top_threshold = active_clients["total_spent_eur"].quantile(0.80)

def segment_client(row):
    """
    Applies custom business rules to classify the customer portfolio.
    Logic priority: Inactivity, Recency risk of churn and Monetary Value
    """
    #Identify leads that registered but have not converted yet
    if row["transaction_count"] == 0:
        return "Inactive / Lead"
    
    #Flagging users with no activity in the last 30 days.
    days_since_last_tx = (latest_date - row["last_transaction"]).days
    if days_since_last_tx > 30:
        return "At Risk"
    
    #Identifying the high revenue "Top Tier"
    if row["total_spent_eur"] >= top_threshold:
        return "Top Tier"
    
    #Default category for active, regular customers
    return "Occasional"

#Applying Logic and Storing Results
client_metrics["segment"] = client_metrics.apply(segment_client, axis=1)

client_metrics.to_sql("client_segments", connect_sql, if_exists="replace", index=False)

print("Analysis completed")
print(client_metrics["segment"].value_counts())
#Close connection with the database
connect_sql.close()