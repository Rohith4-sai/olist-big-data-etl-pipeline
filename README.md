<div align="center">



\# 🚀 Olist Big Data Engineering ETL Pipeline



\### End-to-End Cloud Data Pipeline for E-Commerce Logistics Analytics



\*\*AWS S3 → Apache Airflow → PySpark → Spark SQL → AWS S3\*\*



<br>



!\[Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge\&logo=python\&logoColor=white)

!\[Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?style=for-the-badge\&logo=apacheairflow\&logoColor=white)

!\[Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)

!\[AWS](https://img.shields.io/badge/AWS-S3-232F3E?style=for-the-badge\&logo=amazonaws\&logoColor=white)

!\[Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge\&logo=docker\&logoColor=white)



<br>



A containerized ETL pipeline that automatically ingests Olist e-commerce data from \*\*Amazon S3\*\*, orchestrates processing with \*\*Apache Airflow\*\*, performs distributed transformations using \*\*PySpark and Spark SQL\*\*, and publishes analytics-ready results back to S3.



</div>



\---



\# 📌 Project Overview



E-commerce platforms generate data across orders, customers, sellers, products, payments, and logistics.



This project implements an \*\*end-to-end data engineering pipeline\*\* to process the Brazilian Olist E-Commerce dataset and answer a logistics-focused business question:



> \*\*Which orders were handed over by sellers to the carrier after the required shipping deadline?\*\*



The workflow automates the complete data lifecycle:



```text

Raw Cloud Data

&#x20;     ↓

Data Ingestion

&#x20;     ↓

Workflow Orchestration

&#x20;     ↓

Distributed Processing

&#x20;     ↓

Business Transformation

&#x20;     ↓

Processed Cloud Data

```



The final pipeline successfully identified \*\*10,423 late-shipment records across 13 analytical columns\*\*.



\---



\# 📊 Key Results



<div align="center">



| 🚚 Late-Shipment Records | 📋 Output Columns | ⚙️ Automated Tasks | ☁️ Storage |

|:---:|:---:|:---:|:---:|

| \*\*10,423\*\* | \*\*13\*\* | \*\*3 Airflow Tasks\*\* | \*\*AWS S3\*\* |

| Identified by Spark processing | Analytics-ready schema | End-to-end orchestration | Raw + Processed layers |



</div>



\### ✅ Pipeline Status



```text

DOWNLOAD RAW DATA     ✅ SUCCESS

&#x20;       ↓

PYSPARK PROCESSING    ✅ SUCCESS

&#x20;       ↓

UPLOAD PROCESSED DATA ✅ SUCCESS

```



\---



\# 🏗️ Data Pipeline Architecture



```text

&#x20;                   ┌───────────────────────┐

&#x20;                   │   Olist E-Commerce    │

&#x20;                   │      Dataset          │

&#x20;                   └───────────┬───────────┘

&#x20;                               │

&#x20;                               ▼

&#x20;                   ┌───────────────────────┐

&#x20;                   │       AWS S3          │

&#x20;                   │      RAW LAYER        │

&#x20;                   │      raw-data/        │

&#x20;                   └───────────┬───────────┘

&#x20;                               │

&#x20;                        Python + Boto3

&#x20;                               │

&#x20;                               ▼

&#x20;                   ┌───────────────────────┐

&#x20;                   │    Apache Airflow     │

&#x20;                   │ Workflow Orchestration│

&#x20;                   └───────────┬───────────┘

&#x20;                               │

&#x20;                               ▼

&#x20;                   ┌───────────────────────┐

&#x20;                   │       PySpark         │

&#x20;                   │     + Spark SQL       │

&#x20;                   │                       │

&#x20;                   │ Load • Join • Filter  │

&#x20;                   │      Transform        │

&#x20;                   └───────────┬───────────┘

&#x20;                               │

&#x20;                               ▼

&#x20;                   ┌───────────────────────┐

&#x20;                   │ Late Shipment Analysis│

&#x20;                   │                       │

&#x20;                   │    10,423 Records     │

&#x20;                   │      13 Columns       │

&#x20;                   └───────────┬───────────┘

&#x20;                               │

&#x20;                        Python + Boto3

&#x20;                               │

&#x20;                               ▼

&#x20;                   ┌───────────────────────┐

&#x20;                   │       AWS S3          │

&#x20;                   │   PROCESSED LAYER     │

&#x20;                   │                       │

&#x20;                   │  processed-data/      │

&#x20;                   │ late\_shipments.csv    │

&#x20;                   └───────────────────────┘

```



\---



\# ⚙️ How the Pipeline Works



\## 1️⃣ Data Ingestion — AWS S3



Raw Olist CSV datasets are stored in the AWS S3 raw data layer:



```text

AWS S3

└── raw-data/

```



The first Airflow task uses \*\*Python + Boto3\*\* to download the required datasets into the processing environment.



```text

AWS S3 → Boto3 → Local Processing Environment

```



\---



\## 2️⃣ Distributed Processing — PySpark



The processing stage uses \*\*PySpark\*\* to load and process the datasets.



The primary datasets used in the transformation are:



```text

olist\_order\_items\_dataset.csv

olist\_orders\_dataset.csv

olist\_products\_dataset.csv

```



Spark DataFrames are created and registered as temporary SQL views for transformation.



\---



\## 3️⃣ Transformation — Spark SQL



Spark SQL joins order, product, seller, customer, and logistics-related information.



The core business condition used to identify missed shipping deadlines is:



```sql

shipping\_limit\_date < order\_delivered\_carrier\_date

```



This identifies cases where the seller handed the package to the carrier \*\*after the required shipping deadline\*\*.



The resulting dataset contains information such as:



```text

Order

├── Order ID

├── Customer ID

├── Order Status

└── Purchase Timestamp



Seller / Product

├── Seller ID

├── Product ID

└── Product Category



Logistics

├── Shipping Limit Date

├── Carrier Delivery Date

├── Customer Delivery Date

└── Estimated Delivery Date



Financial

├── Price

└── Freight Value

```



\---



\## 4️⃣ Processed Output



The Spark transformation generated:



```text

╔══════════════════════════════════════╗

║        FINAL PIPELINE RESULT         ║

╠══════════════════════════════════════╣

║ Late Shipment Records  │  10,423    ║

║ Output Columns         │  13        ║

║ Output Format          │  CSV       ║

║ Pipeline Status        │  SUCCESS   ║

╚══════════════════════════════════════╝

```



Final file:



```text

late\_shipments.csv

```



\---



\## 5️⃣ Cloud Data Publishing



The final Airflow task uploads the transformed dataset using \*\*Boto3\*\*:



```text

late\_shipments.csv

&#x20;       ↓

Python + Boto3

&#x20;       ↓

AWS S3

&#x20;       ↓

processed-data/late\_shipments.csv

```



This creates a clear separation between \*\*raw source data\*\* and \*\*processed analytics-ready data\*\*.



\---



\# 🔄 Apache Airflow Orchestration



The complete ETL workflow is controlled by an Apache Airflow DAG.



```text

┌─────────────────────────────┐

│  download\_raw\_data\_from\_s3  │

└──────────────┬──────────────┘

&#x20;              │

&#x20;              ▼

┌─────────────────────────────┐

│   process\_late\_shipments    │

│     PySpark + Spark SQL     │

└──────────────┬──────────────┘

&#x20;              │

&#x20;              ▼

┌─────────────────────────────┐

│ upload\_processed\_data\_to\_s3 │

└─────────────────────────────┘

```



Airflow manages task dependencies so each stage executes only after the previous stage completes successfully.



\---



\# 📸 Pipeline in Action



\## 🟢 Apache Airflow — Successful End-to-End DAG



All three pipeline tasks completed successfully.



!\[Apache Airflow DAG Success](screenshots/airflow-dag-success.png)



\---



\## ☁️ AWS S3 — Processed Data Layer



The final transformed dataset was successfully uploaded to the `processed-data/` layer in Amazon S3.



!\[AWS S3 Processed Output](screenshots/s3-processed-output.png)



\---



\## 📊 ETL Processing Results



The final processed dataset contains \*\*10,423 late-shipment records and 13 columns\*\*.



!\[ETL Pipeline Results](screenshots/etl-results.png)



\---



\# 🛠️ Technology Stack



<div align="center">



| Category | Technologies |

|---|---|

| 🐍 \*\*Programming\*\* | Python |

| 🔥 \*\*Big Data Processing\*\* | PySpark, Apache Spark |

| 🗃️ \*\*Data Transformation\*\* | Spark SQL |

| ⚙️ \*\*Orchestration\*\* | Apache Airflow |

| ☁️ \*\*Cloud Storage\*\* | AWS S3 |

| 🔗 \*\*AWS Integration\*\* | Boto3 |

| 📊 \*\*Data Handling\*\* | Pandas |

| 🐳 \*\*Containerization\*\* | Docker, Docker Compose |

| ☕ \*\*Runtime\*\* | Java 17 |

| 🔀 \*\*Version Control\*\* | Git, GitHub |



</div>



\---



\# ☁️ AWS S3 Data Lake Structure



The project separates source and transformed data into dedicated S3 prefixes.



```text

Amazon S3

│

├── 📂 raw-data/

│   │

│   ├── olist\_customers\_dataset.csv

│   ├── olist\_geolocation\_dataset.csv

│   ├── olist\_order\_items\_dataset.csv

│   ├── olist\_order\_payments\_dataset.csv

│   ├── olist\_order\_reviews\_dataset.csv

│   ├── olist\_orders\_dataset.csv

│   ├── olist\_products\_dataset.csv

│   ├── olist\_sellers\_dataset.csv

│   └── product\_category\_name\_translation.csv

│

└── 📂 processed-data/

&#x20;   │

&#x20;   └── late\_shipments.csv

```



\### Data Flow



```text

RAW LAYER                          PROCESSED LAYER



raw-data/                          processed-data/

&#x20;   │                                    ▲

&#x20;   │                                    │

&#x20;   └──────► AIRFLOW ──► SPARK ─────────┘

```



\---



\# 📁 Project Structure



```text

olist-big-data-etl-pipeline/

│

├── 📂 airflow/

│   │

│   ├── 📂 dags/

│   │   └── late\_shipments\_to\_carrier\_dag.py

│   │

│   └── 📂 scripts/

│       ├── s3\_download.py

│       ├── spark\_missed\_deadline\_job.py

│       └── s3\_upload.py

│

├── 📂 screenshots/

│   ├── airflow-dag-success.png

│   ├── s3-processed-output.png

│   └── etl-results.png

│

├── 🐳 Dockerfile

├── 🐳 docker-compose.yaml

├── .gitignore

└── README.md

```



Large local datasets and generated files are excluded from GitHub.



\---



\# 🐳 Dockerized Environment



The entire processing environment is containerized using Docker.



\### Environment



```text

Apache Airflow 2.10.5

Python 3.11

Java 17

PySpark

Apache Spark

Pandas

Boto3

```



The custom Docker image provides the dependencies required to run Airflow, Spark processing, and AWS integration in one reproducible environment.



\---



\# 🚀 Running the Project



\## Prerequisites



Make sure the following are installed:



```text

Docker Desktop

AWS CLI

Git

```



You also need:



```text

AWS Account

&#x20;    +

Configured AWS Credentials

&#x20;    +

S3 Bucket

&#x20;    +

Olist Raw Dataset

```



\---



\## Step 1 — Configure AWS



Configure your AWS credentials locally:



```bash

aws configure

```



AWS credentials should \*\*never be hard-coded into source code\*\*.



\---



\## Step 2 — Prepare S3 Raw Data



Upload the Olist datasets into:



```text

raw-data/

```



Expected structure:



```text

your-s3-bucket/

└── raw-data/

&#x20;   ├── olist\_orders\_dataset.csv

&#x20;   ├── olist\_order\_items\_dataset.csv

&#x20;   ├── olist\_products\_dataset.csv

&#x20;   └── ...

```



\---



\## Step 3 — Build Docker Image



```bash

docker compose build

```



\---



\## Step 4 — Start the Environment



```bash

docker compose up -d

```



Verify the container:



```bash

docker ps

```



\---



\## Step 5 — Open Apache Airflow



Open the Airflow web interface:



```text

http://localhost:8080

```



Find the DAG:



```text

olist\_data\_pipeline

```



Enable and trigger the pipeline.



\---



\## Step 6 — Pipeline Execution



Airflow automatically executes:



```text

S3 DOWNLOAD

&#x20;    ↓

PYSPARK + SPARK SQL ETL

&#x20;    ↓

S3 UPLOAD

```



A successful execution should show all pipeline tasks in the \*\*Success\*\* state.



\---



\# 📋 Final Dataset Schema



The processed dataset contains \*\*13 columns\*\*:



| # | Column |

|---:|---|

| 1 | `order\_id` |

| 2 | `seller\_id` |

| 3 | `shipping\_limit\_date` |

| 4 | `price` |

| 5 | `freight\_value` |

| 6 | `product\_id` |

| 7 | `product\_category\_name` |

| 8 | `customer\_id` |

| 9 | `order\_status` |

| 10 | `order\_purchase\_timestamp` |

| 11 | `order\_delivered\_carrier\_date` |

| 12 | `order\_delivered\_customer\_date` |

| 13 | `order\_estimated\_delivery\_date` |



\---



\# 🧠 Data Engineering Skills Demonstrated



```text

&#x20;                   DATA ENGINEERING

&#x20;                          │

&#x20;         ┌────────────────┼────────────────┐

&#x20;         │                │                │

&#x20;         ▼                ▼                ▼

&#x20;    INGESTION        PROCESSING      ORCHESTRATION

&#x20;   Python/Boto3       PySpark           Airflow

&#x20;         │                │                │

&#x20;         └────────────────┼────────────────┘

&#x20;                          │

&#x20;                          ▼

&#x20;                   TRANSFORMATION

&#x20;                     Spark SQL

&#x20;                          │

&#x20;                          ▼

&#x20;                   CLOUD STORAGE

&#x20;                      AWS S3

&#x20;                          │

&#x20;                          ▼

&#x20;                   CONTAINERIZATION

&#x20;                       Docker

```



This project demonstrates:



\- End-to-end ETL pipeline development

\- Apache Airflow DAG orchestration

\- PySpark distributed data processing

\- Spark SQL transformations

\- Multi-table data joins

\- Business-rule-based filtering

\- AWS S3 cloud data storage

\- Raw and processed data-layer organization

\- Python/Boto3 AWS integration

\- Docker containerization

\- Pipeline dependency management

\- Git and GitHub version control



\---



\# 🔐 Security Practices



Sensitive credentials are not stored in the repository.



The `.gitignore` excludes local and sensitive files such as:



```text

.env

.env.\*

.aws/

credentials

\*.pem

\*.key

airflow.db

standalone\_admin\_password.txt

Data/

```



AWS credentials are configured locally and accessed by the Dockerized pipeline when required.



\---



\# 📈 Project Highlights



<div align="center">



| Feature | Implementation |

|---|---|

| ☁️ Cloud Storage | AWS S3 raw and processed layers |

| ⚙️ Workflow Automation | Apache Airflow DAG |

| 🔥 Big Data Processing | PySpark |

| 🗃️ SQL Transformation | Spark SQL |

| 🔗 Cloud Integration | Python + Boto3 |

| 🐳 Reproducible Environment | Docker |

| 📊 Business Output | Late-shipment analytics dataset |

| ✅ Verified Result | 10,423 records / 13 columns |



</div>



\---



\# 🔮 Future Enhancements



The pipeline can be extended with:



\- \*\*PostgreSQL\*\* for production-grade Airflow metadata storage

\- \*\*Parquet\*\* instead of CSV for efficient analytical workloads

\- \*\*AWS Glue Data Catalog\*\* for metadata management

\- \*\*Amazon Athena\*\* for serverless SQL analytics

\- \*\*AWS EMR\*\* for distributed cloud Spark processing

\- Automated \*\*data-quality validation\*\*

\- Pipeline monitoring and failure alerts

\- Scheduled incremental data processing

\- Data partitioning for improved query performance

\- BI dashboards for logistics performance analysis



\---



\# 🏁 End-to-End Outcome



```text

&#x20;                    OLIST E-COMMERCE DATA

&#x20;                             │

&#x20;                             ▼

&#x20;                    ☁️ AWS S3 RAW DATA

&#x20;                             │

&#x20;                             ▼

&#x20;                      🐍 PYTHON + BOTO3

&#x20;                             │

&#x20;                             ▼

&#x20;                    ⚙️ APACHE AIRFLOW

&#x20;                             │

&#x20;                             ▼

&#x20;                   🔥 PYSPARK + SPARK SQL

&#x20;                             │

&#x20;                             ▼

&#x20;                   📊 LOGISTICS ANALYSIS

&#x20;                             │

&#x20;                             ▼

&#x20;                   10,423 LATE SHIPMENTS

&#x20;                        13 COLUMNS

&#x20;                             │

&#x20;                             ▼

&#x20;                   ☁️ AWS S3 PROCESSED

&#x20;                             │

&#x20;                             ▼

&#x20;                  ✅ PIPELINE COMPLETED

```



<div align="center">



\### ⭐ Olist Big Data Engineering ETL Pipeline



\*\*Python • PySpark • Spark SQL • Apache Airflow • AWS S3 • Docker\*\*



\*\*Project Status: ✅ Successfully Completed\*\*



</div>

