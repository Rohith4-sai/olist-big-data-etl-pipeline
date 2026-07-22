\# 🚀 Olist Big Data Engineering ETL Pipeline



\### End-to-End Cloud Data Pipeline for E-Commerce Logistics Analytics



\*\*AWS S3 → Apache Airflow → PySpark → Spark SQL → AWS S3\*\*



!\[Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge\&logo=python\&logoColor=white)

!\[PySpark](https://img.shields.io/badge/PySpark-Apache%20Spark-orange?style=for-the-badge\&logo=apachespark\&logoColor=white)

!\[Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-blue?style=for-the-badge\&logo=apacheairflow\&logoColor=white)

!\[AWS S3](https://img.shields.io/badge/AWS-S3-orange?style=for-the-badge\&logo=amazonaws\&logoColor=white)

!\[Docker](https://img.shields.io/badge/Docker-Containerized-blue?style=for-the-badge\&logo=docker\&logoColor=white)

!\[Spark SQL](https://img.shields.io/badge/Spark-SQL-orange?style=for-the-badge\&logo=apachespark\&logoColor=white)



\---



\## 📌 About This Project



This project implements an \*\*end-to-end Big Data Engineering ETL pipeline\*\* using Apache Airflow, PySpark, Spark SQL, AWS S3, Docker, Python, Pandas, and Boto3.



The pipeline processes the \*\*Brazilian Olist E-Commerce dataset\*\* to identify orders where sellers handed packages to the carrier \*\*after the required shipping deadline\*\*.



The complete workflow automatically:



\*\*Downloads raw data from AWS S3 → Orchestrates ETL tasks with Airflow → Processes data using PySpark and Spark SQL → Generates an analytics-ready dataset → Uploads processed results back to AWS S3\*\*



\---



\## 📊 Key Results



| Metric | Result |

|---|---:|

| 🚚 Late-shipment records identified | \*\*10,423\*\* |

| 📋 Final output columns | \*\*13\*\* |

| ⚙️ Automated Airflow tasks | \*\*3\*\* |

| ☁️ Cloud storage | \*\*AWS S3\*\* |

| 📄 Final output | \*\*late\_shipments.csv\*\* |

| ✅ Pipeline execution | \*\*SUCCESS\*\* |



\---



\## 🏗️ Pipeline Architecture



```text

┌──────────────────────────┐

│   Olist E-Commerce Data  │

└────────────┬─────────────┘

&#x20;            │

&#x20;            ▼

┌──────────────────────────┐

│       AWS S3             │

│       RAW LAYER          │

│       raw-data/          │

└────────────┬─────────────┘

&#x20;            │

&#x20;            │ Python + Boto3

&#x20;            ▼

┌──────────────────────────┐

│     Apache Airflow       │

│  Workflow Orchestration  │

└────────────┬─────────────┘

&#x20;            │

&#x20;            ▼

┌──────────────────────────┐

│        PySpark           │

│      + Spark SQL         │

│                          │

│  Load • Join • Filter    │

│       Transform          │

└────────────┬─────────────┘

&#x20;            │

&#x20;            ▼

┌──────────────────────────┐

│  Late Shipment Analysis  │

│                          │

│     10,423 Records       │

│       13 Columns         │

└────────────┬─────────────┘

&#x20;            │

&#x20;            │ Python + Boto3

&#x20;            ▼

┌──────────────────────────┐

│        AWS S3            │

│    PROCESSED LAYER       │

│                          │

│ processed-data/          │

│ late\_shipments.csv       │

└──────────────────────────┘

```



\### 🔄 End-to-End Data Flow



```text

Olist Dataset

&#x20;     │

&#x20;     ▼

☁️ AWS S3 Raw Layer

&#x20;     │

&#x20;     ▼

🐍 Python + Boto3

&#x20;     │

&#x20;     ▼

⚙️ Apache Airflow

&#x20;     │

&#x20;     ▼

🔥 PySpark + Spark SQL

&#x20;     │

&#x20;     ▼

📊 Late Shipment Analysis

&#x20;     │

&#x20;     ▼

☁️ AWS S3 Processed Layer

```



\---



\## ⚙️ Apache Airflow Workflow



The ETL pipeline is orchestrated using an Apache Airflow DAG named:



```text

olist\_data\_pipeline

```



It contains three dependent tasks:



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



\### Pipeline Execution



```text

DOWNLOAD RAW DATA          ✅ SUCCESS

&#x20;       ↓

PROCESS LATE SHIPMENTS     ✅ SUCCESS

&#x20;       ↓

UPLOAD PROCESSED DATA      ✅ SUCCESS

```



Airflow manages the dependencies so each task executes only after the previous task completes successfully.



\---



\# 📸 Project in Action



\## 🟢 1. Apache Airflow — Successful DAG Execution



The complete `olist\_data\_pipeline` executed successfully with all three ETL tasks completing successfully.



!\[Apache Airflow DAG Success](screenshots/airflow-dag-success.png)



\*\*What this proves:\*\*



\- Airflow DAG loaded correctly

\- S3 download task completed

\- PySpark processing completed

\- S3 upload task completed

\- Full pipeline reached \*\*SUCCESS\*\*



\---



\## ☁️ 2. AWS S3 — Processed Data Output



The transformed dataset was successfully uploaded to the AWS S3 processed-data layer.



!\[AWS S3 Processed Output](screenshots/s3-processed-output.png)



Final cloud output:



```text

processed-data/

└── late\_shipments.csv

```



This confirms successful integration between the ETL pipeline and AWS S3.



\---



\## 📊 3. Final ETL Results



The processed dataset contains \*\*10,423 late-shipment records across 13 columns\*\*.



!\[ETL Results](screenshots/etl-results.png)



```text

Rows:     10,423

Columns:  13

Status:   Successfully Processed

```



\---



\# 🛠️ Tech Stack



| Category | Technologies |

|---|---|

| 🐍 Programming | \*\*Python\*\* |

| 🔥 Big Data Processing | \*\*PySpark, Apache Spark\*\* |

| 🗃️ Data Transformation | \*\*Spark SQL\*\* |

| ⚙️ Workflow Orchestration | \*\*Apache Airflow\*\* |

| ☁️ Cloud Storage | \*\*AWS S3\*\* |

| 🔗 AWS Integration | \*\*Boto3\*\* |

| 📊 Data Handling | \*\*Pandas\*\* |

| 🐳 Containerization | \*\*Docker, Docker Compose\*\* |

| ☕ Java Runtime | \*\*Java 17\*\* |

| 🔀 Version Control | \*\*Git, GitHub\*\* |



\---



\# 🔍 How the ETL Pipeline Works



\## 1️⃣ Extract — Download Raw Data from AWS S3



Raw Olist datasets are stored in the S3 raw-data layer.



```text

AWS S3

│

└── raw-data/

&#x20;   ├── olist\_customers\_dataset.csv

&#x20;   ├── olist\_geolocation\_dataset.csv

&#x20;   ├── olist\_order\_items\_dataset.csv

&#x20;   ├── olist\_order\_payments\_dataset.csv

&#x20;   ├── olist\_order\_reviews\_dataset.csv

&#x20;   ├── olist\_orders\_dataset.csv

&#x20;   ├── olist\_products\_dataset.csv

&#x20;   ├── olist\_sellers\_dataset.csv

&#x20;   └── product\_category\_name\_translation.csv

```



The Airflow download task uses \*\*Python and Boto3\*\* to retrieve the required datasets from AWS S3.



```text

AWS S3

&#x20;  ↓

Boto3

&#x20;  ↓

Local Processing Environment

```



\---



\## 2️⃣ Transform — Process Data with PySpark



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



\## 3️⃣ Analyze — Spark SQL



Spark SQL is used to perform data joins, transformations, and business-rule filtering.



The main logistics condition is:



```sql

shipping\_limit\_date < order\_delivered\_carrier\_date

```



This identifies orders where:



```text

Required Shipping Deadline

&#x20;           <

Actual Handover to Carrier

```



Therefore, the seller handed the package to the carrier \*\*after the required shipping deadline\*\*.



\---



\## 4️⃣ Load — Publish Processed Data to AWS S3



After processing, the pipeline generates:



```text

late\_shipments.csv

```



The final Airflow task uploads this dataset back to AWS S3:



```text

PySpark Output

&#x20;     │

&#x20;     ▼

late\_shipments.csv

&#x20;     │

&#x20;     ▼

Python + Boto3

&#x20;     │

&#x20;     ▼

AWS S3

&#x20;     │

&#x20;     ▼

processed-data/late\_shipments.csv

```



\---



\# ☁️ AWS S3 Data Organization



The project separates raw and processed data into different logical layers.



```text

AWS S3 Bucket

│

├── raw-data/

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

└── processed-data/

&#x20;   │

&#x20;   └── late\_shipments.csv

```



\### Data Layer Design



```text

RAW DATA                         PROCESSED DATA



raw-data/                        processed-data/

&#x20;   │                                  ▲

&#x20;   │                                  │

&#x20;   └──► AIRFLOW ─► PYSPARK ─► SQL ───┘

```



\---



\# 📋 Final Dataset Schema



The processed output contains \*\*13 columns\*\*.



| # | Column | Description |

|---:|---|---|

| 1 | `order\_id` | Unique order identifier |

| 2 | `seller\_id` | Seller identifier |

| 3 | `shipping\_limit\_date` | Required seller shipping deadline |

| 4 | `price` | Product price |

| 5 | `freight\_value` | Freight/shipping value |

| 6 | `product\_id` | Product identifier |

| 7 | `product\_category\_name` | Product category |

| 8 | `customer\_id` | Customer identifier |

| 9 | `order\_status` | Current/final order status |

| 10 | `order\_purchase\_timestamp` | Order purchase timestamp |

| 11 | `order\_delivered\_carrier\_date` | Date handed to carrier |

| 12 | `order\_delivered\_customer\_date` | Customer delivery date |

| 13 | `order\_estimated\_delivery\_date` | Estimated delivery date |



\---



\# 📁 Project Structure



```text

olist-big-data-etl-pipeline/

│

├── airflow/

│   │

│   ├── dags/

│   │   └── late\_shipments\_to\_carrier\_dag.py

│   │

│   └── scripts/

│       ├── s3\_download.py

│       ├── spark\_missed\_deadline\_job.py

│       └── s3\_upload.py

│

├── screenshots/

│   ├── airflow-dag-success.png

│   ├── etl-results.png

│   └── s3-processed-output.png

│

├── Dockerfile

├── docker-compose.yaml

├── .gitignore

└── README.md

```



\---



\# 🐳 Dockerized Data Engineering Environment



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



\### Why Docker?



Docker provides:



\- Consistent development environment

\- Dependency isolation

\- Reproducible setup

\- Easier Airflow deployment

\- Integrated Python, Spark, and Java runtime



\---



\# 🚀 How to Run the Project



\## Prerequisites



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



\## Step 1 — Clone the Repository



```bash

git clone https://github.com/Rohith4-sai/olist-big-data-etl-pipeline.git

```



Enter the project directory:



```bash

cd olist-big-data-etl-pipeline

```



\---



\## Step 2 — Configure AWS Credentials



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



\*\*Never hard-code AWS credentials into Python files or commit credentials to GitHub.\*\*



\---



\## Step 3 — Prepare the S3 Raw Data Layer



Upload the Olist CSV datasets to:



```text

raw-data/

```



Example:



```text

your-s3-bucket/

│

└── raw-data/

&#x20;   ├── olist\_orders\_dataset.csv

&#x20;   ├── olist\_order\_items\_dataset.csv

&#x20;   ├── olist\_products\_dataset.csv

&#x20;   └── ...

```



\---



\## Step 4 — Build the Docker Image



```bash

docker compose build

```



\---



\## Step 5 — Start the Environment



```bash

docker compose up -d

```



Check the container:



```bash

docker ps

```



\---



\## Step 6 — Verify Airflow Scheduler



```bash

docker exec olist-airflow airflow jobs check --job-type SchedulerJob

```



Expected result:



```text

Found one alive job.

```



\---



\## Step 7 — Open Apache Airflow



Open:



```text

http://localhost:8080

```



Find:



```text

olist\_data\_pipeline

```



Enable the DAG and manually trigger it.



\---



\## Step 8 — Monitor Pipeline Execution



The pipeline executes:



```text

download\_raw\_data\_from\_s3

&#x20;           │

&#x20;           ▼

process\_late\_shipments

&#x20;           │

&#x20;           ▼

upload\_processed\_data\_to\_s3

```



Wait until all three tasks show:



```text

SUCCESS ✅

```



\---



\## Step 9 — Verify Final Output



The processed file should be available in AWS S3:



```text

processed-data/late\_shipments.csv

```



Expected processed result:



```text

Rows:     10,423

Columns:  13

```



\---



\# 🧠 Skills Demonstrated



\### Data Engineering



\- End-to-end ETL pipeline development

\- Data ingestion

\- Data transformation

\- Data integration

\- Multi-table joins

\- Business-rule filtering

\- Raw and processed data-layer design



\### Big Data



\- PySpark

\- Apache Spark

\- Spark DataFrames

\- Spark SQL



\### Workflow Orchestration



\- Apache Airflow

\- DAG development

\- Task dependencies

\- Pipeline execution monitoring



\### Cloud



\- AWS S3

\- Python Boto3 integration

\- Cloud-based raw and processed data storage



\### DevOps



\- Docker

\- Docker Compose

\- Containerized development environments



\### Software Engineering



\- Python

\- Git

\- GitHub

\- Modular pipeline scripts

\- Environment configuration



\---



\# 🔐 Security Practices



Sensitive information is excluded from version control.



The `.gitignore` prevents local or sensitive files from being committed, including:



```text

.env

.env.\*

\*.pem

\*.key



.aws/

credentials



airflow.db

\*.db

standalone\_admin\_password.txt

logs/

airflow/logs/



Data/



.venv/

venv/

```



AWS credentials are configured locally rather than hard-coded into the source code.



\---



\# 🎯 Business Value



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



\# 📈 Project Highlights



| Feature | Implementation |

|---|---|

| 📥 Data ingestion | AWS S3 + Boto3 |

| ⚙️ Workflow orchestration | Apache Airflow |

| 🔥 Distributed processing | PySpark / Apache Spark |

| 🗃️ Data transformation | Spark SQL |

| ☁️ Cloud storage | AWS S3 |

| 🐳 Containerization | Docker |

| 📊 Processed output | 10,423 records |

| 📋 Output schema | 13 columns |

| 🔄 Automated stages | 3 Airflow tasks |

| ✅ Final status | Successfully executed |



\---



\# 🔮 Future Enhancements



Possible improvements include:



\- Store Airflow metadata in \*\*PostgreSQL\*\* instead of SQLite

\- Convert CSV output to \*\*Parquet\*\*

\- Add \*\*AWS Glue Data Catalog\*\*

\- Query processed datasets using \*\*Amazon Athena\*\*

\- Run distributed Spark workloads using \*\*AWS EMR\*\*

\- Add automated data-quality validation

\- Add failure notifications and monitoring

\- Implement incremental ETL processing

\- Partition processed datasets

\- Build logistics analytics dashboards

\- Add CI/CD for automated testing and deployment



\---



\# 🏁 Final Outcome



```text

&#x20;               OLIST E-COMMERCE DATA

&#x20;                        │

&#x20;                        ▼

&#x20;                ☁️ AWS S3 RAW LAYER

&#x20;                        │

&#x20;                        ▼

&#x20;                 🐍 PYTHON + BOTO3

&#x20;                        │

&#x20;                        ▼

&#x20;                ⚙️ APACHE AIRFLOW

&#x20;                        │

&#x20;                        ▼

&#x20;               🔥 PYSPARK + SPARK SQL

&#x20;                        │

&#x20;                        ▼

&#x20;               📊 LOGISTICS ANALYSIS

&#x20;                        │

&#x20;                        ▼

&#x20;              10,423 LATE-SHIPMENT

&#x20;                    RECORDS

&#x20;                        │

&#x20;                        ▼

&#x20;               ☁️ AWS S3 PROCESSED

&#x20;                        │

&#x20;                        ▼

&#x20;                ✅ PIPELINE SUCCESS

```



\---



\# 👨‍💻 Project



\*\*Olist Big Data Engineering ETL Pipeline\*\*



Built as a hands-on data engineering project demonstrating an end-to-end workflow across \*\*cloud storage, workflow orchestration, distributed data processing, SQL transformation, and containerization\*\*.



\### Core Technologies



\*\*Python • PySpark • Apache Spark • Spark SQL • Apache Airflow • AWS S3 • Boto3 • Docker • Git\*\*



\---



⭐ \*\*If you find this project useful, consider starring the repository.\*\*

