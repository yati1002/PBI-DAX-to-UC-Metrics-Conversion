# PBI-DAX-to-UC-Metrics-Conversion App

This is a lightweight Databricks App which exposese the capabilities of UC Metrics. This project integrates with the Power BI DAX and converts the DAX measures such as Sum , Min , Max , Average , Calculate , Count, Filter into a UC Metrics View. These views can then be integrated with any Databricks features like AI / BI Dashboard , DBSQL , Genie , Alerting to showcase the End to End capabilities of DB platform . 

# Features

- View PBI DAX Measures in DB App.
- Enable Users to convert existing DAX measure to UC Metrics
- Enable Users to add more measures directly in the App
- Perform validation checks and log the errors. 
- Combine all measures into a UC metrics View YAML . Ability to download YAML to local machine
- Use the UC Metrics Views across DB Dashboards , Genie , SQL Queries etc.
  
# Prerequisites
-Databricks workspace with Databricks Apps Enabled.
-Power BI Premium and have a Power BI Report with DAX published.
-CAN USE permission on a Pro or Serverless SQL warehouse.
-Access to Unity Catalog .

# Step by Step Instructions  
1. Create a Power BI Report with basic measures like Sum , Min , Max , Average over a single Unity Catalog Table
2. Deploy the report in Power BI Service
3. Create a table in Databricks Unity Catalog . The table should have with three columns ( Id string , DAX string , Measure string )
   
5. 
6. Clone the Repository (if you haven't already)
7. Generate a PAT token in Databricks 
8. In the App.py , please enter your DBSQL connection string :  Host Name , Http Path . Also enter you PAT token for token variable
9. Deploy the App
10. 

