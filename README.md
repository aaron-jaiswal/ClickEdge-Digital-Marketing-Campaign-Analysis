

Link To Dashboard:
https://public.tableau.com/views/Click-EdgeMarketingCampaignPerformance/Dashboard1?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link



## Background

This project focuses on analyzing 12 Zoom marketing campaigns conducted over a 10-month period, aimed at driving user engagement and growth through online advertising. Each campaign’s performance was meticulously tracked, capturing metrics such as impressions, clicks, signups, and claims. Designed to balance cost efficiency with maximized conversions, these campaigns sought to enhance user acquisition and retention. The ultimate objective was to optimize performance through data-driven insights, enabling more effective budget allocation and delivering improved return on investment (ROI).

Steps
1. Extract, Transform, Load (ETL)
2. Automation
3. Data Validation
4. Analysis

#### ETL Implementation in Snowflake
	1.	Extract:
	•	The raw dataset, Zoom_Marketing_Data.csv, is ingested from a file system or cloud storage.
	•	Apache Spark reads the CSV file and performs schema inference to extract the data.
	•	The dataset includes campaign details, performance metrics, and financial data.
	2.	Transform:
	•	The data is normalized into two logical tables:
	•	Campaigns Table: Stores unique campaign identifiers (Campaign_ID), financial metrics (Total_Cost, ROI), and other summary-level data.
	•	Campaign Performance Table: Contains daily performance metrics (Impressions, Clicks, Signups, etc.) and associated metadata (Date, CPC, CTR, etc.).
	•	Data cleansing ensures no duplicates or missing values in key columns.
	3.	Load:
	•	Using the Snowflake Spark connector, the normalized data is loaded into Snowflake tables:
	•	Campaigns: A dimension table for campaign-level details.
	•	Campaign_Performance: A fact table for daily campaign metrics.
	•	The tables are set to overwrite mode during the initial ETL process to ensure clean data ingestion.

#### Automation with Apache Airflow
To automate the ETL process, Apache Airflow is used to orchestrate the pipeline. The following DAG (Directed Acyclic Graph) defines the workflow:
	1.	DAG Components:
	•	File Ingestion Task: A sensor checks for new marketing data files in the source directory/cloud bucket.
	•	ETL Processing Task: Triggers the Apache Spark job to execute the ETL script.
	•	Data Validation Task: Performs quality checks on the transformed data before loading it into Snowflake.
	•	Snowflake Load Task: Inserts the data into the Snowflake database.
	2.	Automation Features:
	•	Scheduled Runs: The DAG runs daily to ensure timely updates.
	•	Error Handling: Notifications are triggered on task failures via email or Slack.
	•	Scalability: Airflow scales seamlessly to handle large datasets by managing parallel task execution.

#### Data Validation
Data quality is critical for downstream analytics. The following validation steps are integrated into the pipeline:
	1.	Schema Validation:
	•	Ensure the extracted data matches the expected schema.
	•	Validate column names, data types, and null constraints.
	2.	Integrity Checks:
	•	Verify no duplicate Campaign_IDs exist in the Campaigns table.
	•	Ensure referential integrity between the Campaigns and Campaign_Performance tables.
	3.	Threshold and Range Checks:
	•	Confirm key metrics (e.g., Impressions, Clicks, ROI) fall within expected ranges.
	•	Flag anomalies like negative costs or unrealistically high conversion rates.
	4.	Row Count Comparison:
	•	Compare row counts between source and destination to ensure complete data transfer.


#### Key Metrics for Analysis
	•	Impressions: The number of times an ad was displayed to potential customers.
	•	Clicks: The number of times users clicked on the ad.
	•	Signups: The number of users who signed up after clicking the ad.
	•	CPC (Cost Per Click): The average cost the company pays for each click on the ad.
	•	CTR (Click-Through Rate): The percentage of impressions that resulted in clicks.
	•	Conversion Rate: The percentage of clicks that led to claims or conversions.
	•	ROI (Return on Investment): A financial measure representing the return gained relative to the campaign cost.

<img width="1208" alt="Screenshot 2024-10-31 at 11 20 09 AM" src="https://github.com/user-attachments/assets/f6961e90-87e5-44c4-ae95-499829eb868b">



## Findings 
Top ROI Performers:
- Campaign_10 had the highest average ROI of 32,696, followed by Campaign_1 with 31,266 and Campaign_2 with 30,791.
- These campaigns not only showed strong financial returns but also had relatively low costs per signup (Campaign_10: 5.22, Campaign_1: 5.95).

Engagement and Conversion Trends:
- Campaign_1 had the highest CTR at 14.82%, suggesting high engagement relative to impressions.
- Campaign_1 and Campaign_9 also showed strong conversion rates (Campaign_1: 3.06%, Campaign_9: 3.11%), indicating effective engagement-to-signup rates.

Cost Efficiency:
- Campaign_10 and Campaign_8 had relatively lower costs per signup (Campaign_10: 5.22, Campaign_8: 5.12) despite having a higher CPC than average, suggesting high value per signup.
- Campaign_6 and Campaign_12 had higher costs per signup (Campaign_6: 12.61, Campaign_12: 8.88), which may impact profitability despite decent CTRs.

Conversion and ROI Balance:
- Campaign_9 balances high CTR, conversion rate, and a relatively high ROI, suggesting this campaign’s strategy is effective across multiple metrics.




## Business Recommendations

1. Focus on Campaign Quality Over Impressions: Since higher impressions do not necessarily lead to better ROI, prioritize quality audience targeting and engagement strategies over simply increasing impressions. Invest in refining campaign messaging and personalization to drive better returns.

2. Optimize Cost Per Click (CPC) Investments: Higher CPC does not guarantee better click-through rates, suggesting the need to evaluate the effectiveness of ads beyond just spending. Consider testing different ad creatives, targeting methods, and copy to boost CTR without significantly raising costs.

3. Monitor and Adapt to Conversion Rate Trends: Conversion rates vary significantly over time, so regular monitoring is essential. Investigate potential external factors like seasonality or competition and adjust campaigns accordingly. Consider refreshing ads, offers, or targeting strategies when conversion rates begin to dip to maintain consistent performance.
