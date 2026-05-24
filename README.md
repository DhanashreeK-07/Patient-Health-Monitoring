# Real-Time Patient Health Monitoring ETL Pipeline

An end-to-end, cloud-native Data Engineering pipeline built on Microsoft Azure. This project ingests, processes, and visualizes simulated real-time IoT patient telemetry data (heart rate, temperature, blood pressure) to enable continuous health monitoring.

## 🚀 Architecture Overview

This pipeline leverages modern distributed computing and managed cloud services to transition from a local batch-processing script to a scalable streaming architecture.

| Stage | Technology | Role |
| :--- | :--- | :--- |
| **Ingestion** | Azure Event Hubs (Kafka API) | Serves as the high-throughput message broker receiving continuous telemetry data from simulated edge devices. |
| **Processing** | Azure Databricks (PySpark) | Consumes the data stream, filters out invalid readings, and performs micro-batch aggregations on patient metrics. |
| **Storage** | Azure Database for MySQL | Acts as the robust relational serving layer, storing the cleaned and aggregated health records. |
| **Orchestration** | Apache Airflow | Triggers and monitors the distributed Databricks Spark jobs and ingestion scripts. |
| **Visualization** | Python Flask & Azure App Service | Hosts a web dashboard that queries the MySQL database and generates live Matplotlib charts. |

## 📁 Repository Structure

```text
patient-health-monitor/
├── data/
│   └── human_vital_signs_dataset_2024.csv  # Raw simulation data
├── src/
│   ├── producer_azure.py                   # Event Hubs ingestion script
│   ├── spark_consumer_azure.py             # Databricks Spark streaming job
│   └── app_azure.py                        # Flask dashboard application
├── dags/
│   └── etl_dag_azure.py                    # Airflow orchestration script
├── requirements.txt                        # Python dependencies
├── .env.example                            # Template for environment variables
└── README.md                               # Project documentation
