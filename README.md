# Fintech Customer Analytics and Churn Risk Engine
<img width="1174" height="659" alt="Dashboard_preview" src="https://github.com/user-attachments/assets/60bc27d4-3468-4c7b-acc6-ae46af7adbd0" />

### Business Objective
This project solves a core challenge in global Fintech: **normalizing fragmented transaction data across multiple currencies and identifying customer churn risk. Financial institutions process large volumes of transactions daily. Understanding these transactions is key for: detecting unusual or potentially risky behavior, identifying high-value customers, improving operational and business decisions** I built an end-to-end data pipeline that cleans duplicate logs, handles FX rate gaps (weekends/holidays) using a Forward Fill strategy, and classifies 100% of the customer portfolio (including inactive leads) using a custom **Risk Scoring Engine**.

---

### Tech Stack
* **SQL (SQLite):** Data cleaning, Window Functions, and Forward Fill logic for FX rates.
* **Python (Pandas):** Statistical segmentation and Feature Engineering.
* **Power BI:** Interactive Dashboard for Business Intelligence and KPI tracking.
* 
---

### Data Pipeline
#### 1. SQL Layer (Data Integrity and Normalization)
* **De-duplication:** Used `ROW_NUMBER()` to enforce uniqueness on Transaction IDs.
* **FX Forward Fill:** Implemented a "Last Known Value" logic using `COUNT() OVER` and `FIRST_VALUE` to handle missing exchange rates on weekends/holidays.
* **Base Currency Conversion:** Standardized all transactions (JPY, GBP, USD) to **EUR** to ensure a "single version of the truth" for financial reporting.

#### 2. Python Engine (Strategic Segmentation)
* **Recency Logic:** Calculated the gap between the last transaction and the reporting date to flag users with >30 days of inactivity as **"At Risk"**.
* **Monetary Scoring:** Used the **80th percentile (Pareto Principle)** to identify "Top Tier" clients, excluding inactive leads to avoid statistical skew.
* **Full Portfolio Coverage:** Handled missing values (Imputation) to ensure that even customers with 0 transactions are tracked as **"Inactive Leads"**.
  
---

### Business Insights
* **Revenue:** **Germany** is the country that has the highest total amount of transactions accounting **23%** of total revenue, Recommend marketing campaign to transform ocassional clients to top tier as Germany is our target market for having more transactions and opportunity to convert from ocassional to top tier clients.
* Recommend fidelity program in **France** as most of our top tier clients are from France.
* **Churn Alert:** Identified a **17.02% Churn Rate**, providing the marketing team with a specific list of **16** "At Risk" customers for re-engagement.
* **Portfolio Mix:** 19% of the database consists of high-value "Top Tier" customers, while 6% are leads that haven't converted yet.
  
---

### How to Run
1.  Run `Database_logic.sql` to initialize the cleaned views.
2.  Execute `Risk_analysis_engine.py` to generate the segments.
3.  Open `Fintech_dashboard.pbix` to view the interactive insights.

---

### Limitations
* The dataset is static and does not reflect real-time transaction processing
* No advanced machine learning models were applied
* Results are exploratory and would need further validation in a real-world setting.

---

### Conclusion
This project demonstrates a structured approach to analyzing financial transactions and extracting insights that can support business decisions. It highlights the importance of moving beyond descriptive analysis toward more actionable insights.

**Note: To comply with data protection standards, all datasets used in this project were synthetically generated. The data simulates real-world fintech transaction patterns, including currency fluctuations and customer behavioral trends.**

---

**Author:** Daniel Alejandro Marin Gil  
**Tools:** SQL, Jupyter notebook Python, Power BI, Excel.
