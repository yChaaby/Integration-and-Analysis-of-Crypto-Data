# Integration and Analysis of Crypto Data

This project demonstrates the integration, processing, and analysis of cryptocurrency data using modern tools and platforms like **AWS Lambda**, **Apache Airflow**, **Snowflake**, and **Tableau**. The pipeline is designed to process real-time data, store it securely, and analyze it efficiently.

## Pipeline Overview

![Pipeline Diagram](./Diagramme%20sans%20nom.drawio%20(1).png)

### Steps

1. **Data Integration**: Real-time cryptocurrency data is fetched from an API.
2. **AWS Lambda**: Processes the incoming data and stores it in AWS S3.
3. **Apache Airflow**: Automates ETL workflows, extracting data from S3, transforming it, and loading it into Snowflake.
4. **Snowflake**: Serves as a scalable data warehouse for structured querying and analysis.
5. **Tableau**: Connects to Snowflake to visualize cryptocurrency trends and generate dashboards.

## Key Tools

- **AWS Lambda & S3**: Real-time data processing and secure storage.
- **Apache Airflow**: Workflow orchestration and automation.
- **Snowflake**: Cloud-based data warehousing for analytics.
- **Tableau**: Interactive data visualization and dashboard creation.

## Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/Integration-and-Analysis-of-Crypto-Data.git
   cd Integration-and-Analysis-of-Crypto-Data

Diagramme sans nom.drawio (1).png

