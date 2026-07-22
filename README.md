# Olist Big Data Engineering ETL Pipeline



### End-to-End Cloud Data Pipeline for E-Commerce Logistics Analytics



**AWS S3 | Apache Airflow | PySpark | Spark SQL | AWS S3**



![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python&logoColor=white)

![PySpark](https://img.shields.io/badge/PySpark-Apache%20Spark-orange?style=for-the-badge&logo=apachespark&logoColor=white)

![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-blue?style=for-the-badge&logo=apacheairflow&logoColor=white)

![AWS S3](https://img.shields.io/badge/AWS-S3-orange?style=for-the-badge&logo=amazonaws&logoColor=white)

![Docker](https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge&logo=docker&logoColor=white)

![Spark SQL](https://img.shields.io/badge/Spark-SQL-orange?style=for-the-badge&logo=apachespark&logoColor=white)



\---



## Ã°Å¸â€œÅ’ About This Project



This project implements an **end-to-end Big Data Engineering ETL pipeline** using Apache Airflow, PySpark, Spark SQL, AWS S3, Docker, Python, Pandas, and Boto3.



The pipeline processes the **Brazilian Olist E-Commerce dataset** to identify orders where sellers handed packages to the carrier **after the required shipping deadline**.



The complete workflow automatically:



**Downloads raw data from AWS S3 Ã¢â€ â€™ Orchestrates ETL tasks with Airflow Ã¢â€ â€™ Processes data using PySpark and Spark SQL Ã¢â€ â€™ Generates an analytics-ready dataset Ã¢â€ â€™ Uploads processed results back to AWS S3**



\---



## Ã°Å¸â€œÅ  Key Results



| Metric | Result |

|---|---:|

| Ã°Å¸Å¡Å¡ Late-shipment records identified | **10,423** |

| Ã°Å¸â€œâ€¹ Final output columns | **13** |

| Ã¢Å¡â„¢Ã¯Â¸Â Automated Airflow tasks | **3** |

| Ã¢ËœÂÃ¯Â¸Â Cloud storage | **AWS S3** |

| Ã°Å¸â€œâ€ž Final output | **late_shipments.csv** |

| Ã¢Å“â€¦ Pipeline execution | **SUCCESS** |



\---



## Ã°Å¸Ââ€”Ã¯Â¸Â Pipeline Architecture



```text

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š   Olist E-Commerce Data  Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

&#x20;            Ã¢â€â€š

&#x20;            Ã¢â€“Â¼

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š       AWS S3             Ã¢â€â€š

Ã¢â€â€š       RAW LAYER          Ã¢â€â€š

Ã¢â€â€š       raw-data/          Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

&#x20;            Ã¢â€â€š

&#x20;            Ã¢â€â€š Python + Boto3

&#x20;            Ã¢â€“Â¼

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š     Apache Airflow       Ã¢â€â€š

Ã¢â€â€š  Workflow Orchestration  Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

&#x20;            Ã¢â€â€š

&#x20;            Ã¢â€“Â¼

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š        PySpark           Ã¢â€â€š

Ã¢â€â€š      + Spark SQL         Ã¢â€â€š

Ã¢â€â€š                          Ã¢â€â€š

Ã¢â€â€š  Load Ã¢â‚¬Â¢ Join Ã¢â‚¬Â¢ Filter    Ã¢â€â€š

Ã¢â€â€š       Transform          Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

&#x20;            Ã¢â€â€š

&#x20;            Ã¢â€“Â¼

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š  Late Shipment Analysis  Ã¢â€â€š

Ã¢â€â€š                          Ã¢â€â€š

Ã¢â€â€š     10,423 Records       Ã¢â€â€š

Ã¢â€â€š       13 Columns         Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

&#x20;            Ã¢â€â€š

&#x20;            Ã¢â€â€š Python + Boto3

&#x20;            Ã¢â€“Â¼

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š        AWS S3            Ã¢â€â€š

Ã¢â€â€š    PROCESSED LAYER       Ã¢â€â€š

Ã¢â€â€š                          Ã¢â€â€š

Ã¢â€â€š processed-data/          Ã¢â€â€š

Ã¢â€â€š late_shipments.csv       Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

```



### Ã°Å¸â€â€ž End-to-End Data Flow



```text

Olist Dataset

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

Ã¢ËœÂÃ¯Â¸Â AWS S3 Raw Layer

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

Ã°Å¸ÂÂ Python + Boto3

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

Ã¢Å¡â„¢Ã¯Â¸Â Apache Airflow

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

Ã°Å¸â€Â¥ PySpark + Spark SQL

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

Ã°Å¸â€œÅ  Late Shipment Analysis

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

Ã¢ËœÂÃ¯Â¸Â AWS S3 Processed Layer

```



\---



## Ã¢Å¡â„¢Ã¯Â¸Â Apache Airflow Workflow



The ETL pipeline is orchestrated using an Apache Airflow DAG named:



```text

olist_data_pipeline

```



It contains three dependent tasks:



```text

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š  download_raw_data_from_s3  Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

&#x20;              Ã¢â€â€š

&#x20;              Ã¢â€“Â¼

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š   process_late_shipments    Ã¢â€â€š

Ã¢â€â€š     PySpark + Spark SQL     Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

&#x20;              Ã¢â€â€š

&#x20;              Ã¢â€“Â¼

Ã¢â€Å’Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Â

Ã¢â€â€š upload_processed_data_to_s3 Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

```



### Pipeline Execution



```text

DOWNLOAD RAW DATA          Ã¢Å“â€¦ SUCCESS

&#x20;       Ã¢â€ â€œ

PROCESS LATE SHIPMENTS     Ã¢Å“â€¦ SUCCESS

&#x20;       Ã¢â€ â€œ

UPLOAD PROCESSED DATA      Ã¢Å“â€¦ SUCCESS

```



Airflow manages the dependencies so each task executes only after the previous task completes successfully.



\---



# Ã°Å¸â€œÂ¸ Project in Action



## Ã°Å¸Å¸Â¢ 1. Apache Airflow Ã¢â‚¬â€ Successful DAG Execution



The complete `olist_data_pipeline` executed successfully with all three ETL tasks completing successfully.



![Apache Airflow DAG Success](screenshots/airflow-dag-success.png)



**What this proves:**



\- Airflow DAG loaded correctly

\- S3 download task completed

\- PySpark processing completed

\- S3 upload task completed

\- Full pipeline reached **SUCCESS**



\---



## Ã¢ËœÂÃ¯Â¸Â 2. AWS S3 Ã¢â‚¬â€ Processed Data Output



The transformed dataset was successfully uploaded to the AWS S3 processed-data layer.



![AWS S3 Processed Output](screenshots/s3-processed-output.png)



Final cloud output:



```text

processed-data/

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ late_shipments.csv

```



This confirms successful integration between the ETL pipeline and AWS S3.



\---



## Ã°Å¸â€œÅ  3. Final ETL Results



The processed dataset contains **10,423 late-shipment records across 13 columns**.



![ETL Results](screenshots/etl-results.png)



```text

Rows:     10,423

Columns:  13

Status:   Successfully Processed

```



\---



# Ã°Å¸â€ºÂ Ã¯Â¸Â Tech Stack



| Category | Technologies |

|---|---|

| Ã°Å¸ÂÂ Programming | **Python** |

| Ã°Å¸â€Â¥ Big Data Processing | **PySpark, Apache Spark** |

| Ã°Å¸â€”Æ’Ã¯Â¸Â Data Transformation | **Spark SQL** |

| Ã¢Å¡â„¢Ã¯Â¸Â Workflow Orchestration | **Apache Airflow** |

| Ã¢ËœÂÃ¯Â¸Â Cloud Storage | **AWS S3** |

| Ã°Å¸â€â€” AWS Integration | **Boto3** |

| Ã°Å¸â€œÅ  Data Handling | **Pandas** |

| Ã°Å¸ÂÂ³ Containerization | **Docker, Docker Compose** |

| Ã¢Ëœâ€¢ Java Runtime | **Java 17** |

| Ã°Å¸â€â‚¬ Version Control | **Git, GitHub** |



\---



# Ã°Å¸â€Â How the ETL Pipeline Works



## 1Ã¯Â¸ÂÃ¢Æ’Â£ Extract Ã¢â‚¬â€ Download Raw Data from AWS S3



Raw Olist datasets are stored in the S3 raw-data layer.



```text

AWS S3

Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ raw-data/

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_customers_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_geolocation_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_order_items_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_order_payments_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_order_reviews_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_orders_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_products_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_sellers_dataset.csv

&#x20;   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ product_category_name_translation.csv

```



The Airflow download task uses **Python and Boto3** to retrieve the required datasets from AWS S3.



```text

AWS S3

&#x20;  Ã¢â€ â€œ

Boto3

&#x20;  Ã¢â€ â€œ

Local Processing Environment

```



\---



## 2Ã¯Â¸ÂÃ¢Æ’Â£ Transform Ã¢â‚¬â€ Process Data with PySpark



PySpark loads and processes the Olist datasets using Spark DataFrames.



The pipeline combines information related to:



\- Orders

\- Order items

\- Products

\- Sellers

\- Customers

\- Shipping deadlines

\- Carrier delivery timestamps

\- Product categories

\- Price and freight information



Spark provides the processing engine used for the transformation stage.



\---



## 3Ã¯Â¸ÂÃ¢Æ’Â£ Analyze Ã¢â‚¬â€ Spark SQL



Spark SQL is used to perform data joins, transformations, and business-rule filtering.



The main logistics condition is:



```sql

shipping_limit_date < order_delivered_carrier_date

```



This identifies orders where:



```text

Required Shipping Deadline

&#x20;           <

Actual Handover to Carrier

```



Therefore, the seller handed the package to the carrier **after the required shipping deadline**.



\---



## 4Ã¯Â¸ÂÃ¢Æ’Â£ Load Ã¢â‚¬â€ Publish Processed Data to AWS S3



After processing, the pipeline generates:



```text

late_shipments.csv

```



The final Airflow task uploads this dataset back to AWS S3:



```text

PySpark Output

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

late_shipments.csv

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

Python + Boto3

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

AWS S3

&#x20;     Ã¢â€â€š

&#x20;     Ã¢â€“Â¼

processed-data/late_shipments.csv

```



\---



# Ã¢ËœÂÃ¯Â¸Â AWS S3 Data Organization



The project separates raw and processed data into different logical layers.



```text

AWS S3 Bucket

Ã¢â€â€š

Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ raw-data/

Ã¢â€â€š   Ã¢â€â€š

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_customers_dataset.csv

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_geolocation_dataset.csv

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_order_items_dataset.csv

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_order_payments_dataset.csv

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_order_reviews_dataset.csv

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_orders_dataset.csv

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_products_dataset.csv

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_sellers_dataset.csv

Ã¢â€â€š   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ product_category_name_translation.csv

Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ processed-data/

&#x20;   Ã¢â€â€š

&#x20;   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ late_shipments.csv

```



### Data Layer Design



```text

RAW DATA                         PROCESSED DATA



raw-data/                        processed-data/

&#x20;   Ã¢â€â€š                                  Ã¢â€“Â²

&#x20;   Ã¢â€â€š                                  Ã¢â€â€š

&#x20;   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€“Âº AIRFLOW Ã¢â€â‚¬Ã¢â€“Âº PYSPARK Ã¢â€â‚¬Ã¢â€“Âº SQL Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€Ëœ

```



\---



# Ã°Å¸â€œâ€¹ Final Dataset Schema



The processed output contains **13 columns**.



| # | Column | Description |

|---:|---|---|

| 1 | `order_id` | Unique order identifier |

| 2 | `seller_id` | Seller identifier |

| 3 | `shipping_limit_date` | Required seller shipping deadline |

| 4 | `price` | Product price |

| 5 | `freight_value` | Freight/shipping value |

| 6 | `product_id` | Product identifier |

| 7 | `product_category_name` | Product category |

| 8 | `customer_id` | Customer identifier |

| 9 | `order_status` | Current/final order status |

| 10 | `order_purchase_timestamp` | Order purchase timestamp |

| 11 | `order_delivered_carrier_date` | Date handed to carrier |

| 12 | `order_delivered_customer_date` | Customer delivery date |

| 13 | `order_estimated_delivery_date` | Estimated delivery date |



\---



# Ã°Å¸â€œÂ Project Structure



```text

olist-big-data-etl-pipeline/

Ã¢â€â€š

Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ airflow/

Ã¢â€â€š   Ã¢â€â€š

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ dags/

Ã¢â€â€š   Ã¢â€â€š   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ late_shipments_to_carrier_dag.py

Ã¢â€â€š   Ã¢â€â€š

Ã¢â€â€š   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ scripts/

Ã¢â€â€š       Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ s3_download.py

Ã¢â€â€š       Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ spark_missed_deadline_job.py

Ã¢â€â€š       Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ s3_upload.py

Ã¢â€â€š

Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ screenshots/

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ airflow-dag-success.png

Ã¢â€â€š   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ etl-results.png

Ã¢â€â€š   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ s3-processed-output.png

Ã¢â€â€š

Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ Dockerfile

Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ docker-compose.yaml

Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ .gitignore

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ README.md

```



\---



# Ã°Å¸ÂÂ³ Dockerized Data Engineering Environment



The project uses Docker to provide a reproducible environment for the pipeline.



The custom Docker environment includes:



```text

Apache Airflow

&#x20;     +

Python 3.11

&#x20;     +

Java 17

&#x20;     +

PySpark / Apache Spark

&#x20;     +

Pandas

&#x20;     +

Boto3

```



### Why Docker?



Docker provides:



\- Consistent development environment

\- Dependency isolation

\- Reproducible setup

\- Easier Airflow deployment

\- Integrated Python, Spark, and Java runtime



\---



# Ã°Å¸Å¡â‚¬ How to Run the Project



## Prerequisites



Install:



```text

Docker Desktop

AWS CLI

Git

```



You also need access to:



```text

AWS Account

AWS S3 Bucket

AWS Credentials

Olist Dataset

```



\---



## Step 1 Ã¢â‚¬â€ Clone the Repository



```bash

git clone https://github.com/Rohith4-sai/olist-big-data-etl-pipeline.git

```



Enter the project directory:



```bash

cd olist-big-data-etl-pipeline

```



\---



## Step 2 Ã¢â‚¬â€ Configure AWS Credentials



Configure AWS locally:



```bash

aws configure

```



Enter your:



```text

AWS Access Key ID

AWS Secret Access Key

Default Region

Output Format

```



**Never hard-code AWS credentials into Python files or commit credentials to GitHub.**



\---



## Step 3 Ã¢â‚¬â€ Prepare the S3 Raw Data Layer



Upload the Olist CSV datasets to:



```text

raw-data/

```



Example:



```text

your-s3-bucket/

Ã¢â€â€š

Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ raw-data/

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_orders_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_order_items_dataset.csv

&#x20;   Ã¢â€Å“Ã¢â€â‚¬Ã¢â€â‚¬ olist_products_dataset.csv

&#x20;   Ã¢â€â€Ã¢â€â‚¬Ã¢â€â‚¬ ...

```



\---



## Step 4 Ã¢â‚¬â€ Build the Docker Image



```bash

docker compose build

```



\---



## Step 5 Ã¢â‚¬â€ Start the Environment



```bash

docker compose up -d

```



Check the container:



```bash

docker ps

```



\---



## Step 6 Ã¢â‚¬â€ Verify Airflow Scheduler



```bash

docker exec olist-airflow airflow jobs check --job-type SchedulerJob

```



Expected result:



```text

Found one alive job.

```



\---



## Step 7 Ã¢â‚¬â€ Open Apache Airflow



Open:



```text

http://localhost:8080

```



Find:



```text

olist_data_pipeline

```



Enable the DAG and manually trigger it.



\---



## Step 8 Ã¢â‚¬â€ Monitor Pipeline Execution



The pipeline executes:



```text

download_raw_data_from_s3

&#x20;           Ã¢â€â€š

&#x20;           Ã¢â€“Â¼

process_late_shipments

&#x20;           Ã¢â€â€š

&#x20;           Ã¢â€“Â¼

upload_processed_data_to_s3

```



Wait until all three tasks show:



```text

SUCCESS Ã¢Å“â€¦

```



\---



## Step 9 Ã¢â‚¬â€ Verify Final Output



The processed file should be available in AWS S3:



```text

processed-data/late_shipments.csv

```



Expected processed result:



```text

Rows:     10,423

Columns:  13

```



\---



# Ã°Å¸Â§Â  Skills Demonstrated



### Data Engineering



\- End-to-end ETL pipeline development

\- Data ingestion

\- Data transformation

\- Data integration

\- Multi-table joins

\- Business-rule filtering

\- Raw and processed data-layer design



### Big Data



\- PySpark

\- Apache Spark

\- Spark DataFrames

\- Spark SQL



### Workflow Orchestration



\- Apache Airflow

\- DAG development

\- Task dependencies

\- Pipeline execution monitoring



### Cloud



\- AWS S3

\- Python Boto3 integration

\- Cloud-based raw and processed data storage



### DevOps



\- Docker

\- Docker Compose

\- Containerized development environments



### Software Engineering



\- Python

\- Git

\- GitHub

\- Modular pipeline scripts

\- Environment configuration



\---



# Ã°Å¸â€Â Security Practices



Sensitive information is excluded from version control.



The `.gitignore` prevents local or sensitive files from being committed, including:



```text

.env

.env.*

*.pem

*.key



.aws/

credentials



airflow.db

*.db

standalone_admin_password.txt

logs/

airflow/logs/



Data/



.venv/

venv/

```



AWS credentials are configured locally rather than hard-coded into the source code.



\---



# Ã°Å¸Å½Â¯ Business Value



Late handover of packages to logistics carriers can contribute to:



\- Delivery delays

\- Poor customer experience

\- Increased customer complaints

\- Lower seller performance

\- Logistics inefficiencies



This pipeline creates an analytics-ready dataset that can be used to investigate:



```text

Which sellers frequently miss shipping deadlines?



Which product categories experience more delays?



How much revenue is associated with late shipments?



Are certain time periods associated with higher delays?



How does late carrier handover affect final customer delivery?

```



\---



# Ã°Å¸â€œË† Project Highlights



| Feature | Implementation |

|---|---|

| Ã°Å¸â€œÂ¥ Data ingestion | AWS S3 + Boto3 |

| Ã¢Å¡â„¢Ã¯Â¸Â Workflow orchestration | Apache Airflow |

| Ã°Å¸â€Â¥ Distributed processing | PySpark / Apache Spark |

| Ã°Å¸â€”Æ’Ã¯Â¸Â Data transformation | Spark SQL |

| Ã¢ËœÂÃ¯Â¸Â Cloud storage | AWS S3 |

| Ã°Å¸ÂÂ³ Containerization | Docker |

| Ã°Å¸â€œÅ  Processed output | 10,423 records |

| Ã°Å¸â€œâ€¹ Output schema | 13 columns |

| Ã°Å¸â€â€ž Automated stages | 3 Airflow tasks |

| Ã¢Å“â€¦ Final status | Successfully executed |



\---



# Ã°Å¸â€Â® Future Enhancements



Possible improvements include:



\- Store Airflow metadata in **PostgreSQL** instead of SQLite

\- Convert CSV output to **Parquet**

\- Add **AWS Glue Data Catalog**

\- Query processed datasets using **Amazon Athena**

\- Run distributed Spark workloads using **AWS EMR**

\- Add automated data-quality validation

\- Add failure notifications and monitoring

\- Implement incremental ETL processing

\- Partition processed datasets

\- Build logistics analytics dashboards

\- Add CI/CD for automated testing and deployment



\---



# Ã°Å¸ÂÂ Final Outcome



```text

&#x20;               OLIST E-COMMERCE DATA

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;                Ã¢ËœÂÃ¯Â¸Â AWS S3 RAW LAYER

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;                 Ã°Å¸ÂÂ PYTHON + BOTO3

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;                Ã¢Å¡â„¢Ã¯Â¸Â APACHE AIRFLOW

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;               Ã°Å¸â€Â¥ PYSPARK + SPARK SQL

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;               Ã°Å¸â€œÅ  LOGISTICS ANALYSIS

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;              10,423 LATE-SHIPMENT

&#x20;                    RECORDS

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;               Ã¢ËœÂÃ¯Â¸Â AWS S3 PROCESSED

&#x20;                        Ã¢â€â€š

&#x20;                        Ã¢â€“Â¼

&#x20;                Ã¢Å“â€¦ PIPELINE SUCCESS

```



\---



# Ã°Å¸â€˜Â¨Ã¢â‚¬ÂÃ°Å¸â€™Â» Project



**Olist Big Data Engineering ETL Pipeline**



Built as a hands-on data engineering project demonstrating an end-to-end workflow across **cloud storage, workflow orchestration, distributed data processing, SQL transformation, and containerization**.



### Core Technologies



**Python Ã¢â‚¬Â¢ PySpark Ã¢â‚¬Â¢ Apache Spark Ã¢â‚¬Â¢ Spark SQL Ã¢â‚¬Â¢ Apache Airflow Ã¢â‚¬Â¢ AWS S3 Ã¢â‚¬Â¢ Boto3 Ã¢â‚¬Â¢ Docker Ã¢â‚¬Â¢ Git**



\---



Ã¢Â­Â **If you find this project useful, consider starring the repository.**

