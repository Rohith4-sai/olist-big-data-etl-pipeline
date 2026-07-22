\# Olist Big Data Engineering ETL Pipeline



An end-to-end data engineering project using \*\*Apache Airflow, PySpark, Spark SQL, AWS S3, Docker, Python, Pandas, and Boto3\*\*.



The pipeline processes the Brazilian Olist E-Commerce dataset to identify orders where sellers handed packages to the carrier after the required shipping deadline.



The complete workflow automatically downloads raw datasets from AWS S3, processes multiple datasets using PySpark and Spark SQL, generates an analytics-ready CSV file, and uploads the processed result back to AWS S3.



\---



\## Project Overview



E-commerce platforms generate large amounts of data across orders, customers, products, sellers, payments, and logistics.



This project demonstrates how an ETL pipeline can automate the movement and transformation of e-commerce data using cloud storage, workflow orchestration, distributed data processing, and containerization.



\### Business Question



\*\*Which orders were handed over to the carrier after the required shipping deadline?\*\*



\### Final Result



\- \*\*10,423 late-shipment records identified\*\*

\- \*\*13 columns in the final processed dataset\*\*

\- Multiple Olist datasets joined using \*\*PySpark and Spark SQL\*\*

\- Complete workflow orchestrated using \*\*Apache Airflow\*\*

\- Raw and processed datasets stored separately in \*\*AWS S3\*\*



\---



\## Architecture



```text

Olist E-Commerce Dataset

&#x20;         |

&#x20;         v

&#x20;      AWS S3

&#x20;     raw-data/

&#x20;         |

&#x20;         v

&#x20;  Apache Airflow

&#x20;         |

&#x20;         v

Python + Boto3

Download Raw Data

&#x20;         |

&#x20;         v

PySpark + Spark SQL

&#x20;         |

&#x20;         v

Join + Transform + Filter

Orders + Order Items + Products

&#x20;         |

&#x20;         v

late\_shipments.csv

10,423 Records

13 Columns

&#x20;         |

&#x20;         v

Python + Boto3

&#x20;         |

&#x20;         v

&#x20;      AWS S3

&#x20;  processed-data/

&#x20;  late\_shipments.csv

```



\---



\## Technology Stack



\- Python

\- PySpark

\- Spark SQL

\- Apache Airflow

\- AWS S3

\- Boto3

\- Docker

\- Docker Compose

\- Pandas

\- Java 17

\- Git

\- GitHub



\---



\## ETL Pipeline



The Apache Airflow DAG orchestrates three main tasks:



```text

download\_raw\_data\_from\_s3

&#x20;         |

&#x20;         v

process\_late\_shipments

&#x20;         |

&#x20;         v

upload\_processed\_data\_to\_s3

```



Each task must complete successfully before the next task begins.



\---



\## 1. Download Raw Data from AWS S3



The first Airflow task uses \*\*Python and Boto3\*\* to connect to AWS S3.



Raw Olist CSV datasets are downloaded from:



```text

raw-data/

```



into the Dockerized Airflow environment for processing.



\---



\## 2. Process Data with PySpark and Spark SQL



The second task performs the main ETL transformation.



A local Spark session is created using PySpark.



The transformation uses three primary datasets:



```text

olist\_order\_items\_dataset.csv

olist\_orders\_dataset.csv

olist\_products\_dataset.csv

```



The datasets are loaded into Spark DataFrames and registered as temporary SQL views.



Spark SQL is then used to join the datasets and identify late seller-to-carrier shipments.



The main business condition is:



```text

shipping\_limit\_date < order\_delivered\_carrier\_date

```



This means the seller handed the product to the carrier after the required shipping deadline.



The transformation combines information including:



\- Order ID

\- Seller ID

\- Product ID

\- Product category

\- Customer ID

\- Shipping deadline

\- Carrier delivery date

\- Customer delivery date

\- Estimated delivery date

\- Order status

\- Purchase timestamp

\- Price

\- Freight value



\---



\## 3. Generate Processed Dataset



After the Spark transformation completes, the final analytics dataset is generated as:



```text

late\_shipments.csv

```



The successfully executed pipeline produced:



```text

Rows:    10,423

Columns: 13

```



\---



\## 4. Upload Processed Data to AWS S3



The final Airflow task uses Python and Boto3 to upload the transformed dataset back to AWS S3.



Final location:



```text

processed-data/late\_shipments.csv

```



This separates raw source data from processed analytics-ready data.



\---



\## AWS S3 Data Lake Structure



The project uses AWS S3 as a simple cloud-based data lake.



```text

S3 Bucket

|

|-- raw-data/

|   |

|   |-- olist\_customers\_dataset.csv

|   |-- olist\_geolocation\_dataset.csv

|   |-- olist\_order\_items\_dataset.csv

|   |-- olist\_order\_payments\_dataset.csv

|   |-- olist\_order\_reviews\_dataset.csv

|   |-- olist\_orders\_dataset.csv

|   |-- olist\_products\_dataset.csv

|   |-- olist\_sellers\_dataset.csv

|   |-- product\_category\_name\_translation.csv

|

|-- processed-data/

&#x20;   |

&#x20;   |-- late\_shipments.csv

```



\---



\## Project Structure



```text

.

|-- airflow/

|   |

|   |-- dags/

|   |   |

|   |   |-- olist\_data\_pipeline.py

|   |

|   |-- scripts/

|       |

|       |-- s3\_download.py

|       |-- spark\_missed\_deadline\_job.py

|       |-- s3\_upload.py

|

|-- Dockerfile

|-- docker-compose.yaml

|-- .gitignore

|-- LICENSE

|-- README.md

```



The large local `Data/` directory is excluded from GitHub.



Raw datasets are stored in AWS S3 instead of being committed to the repository.



\---



\## Docker Environment



The project runs Apache Airflow and its processing dependencies inside Docker.



The custom Docker environment includes:



```text

Apache Airflow 2.10.5

Python 3.11

Java 17

PySpark

Spark SQL

Pandas

Boto3

```



Docker provides an isolated and reproducible environment for running the complete pipeline.



\---



\## Dockerfile



The Docker image is based on Apache Airflow and installs Java and the Python dependencies required for Spark and AWS integration.



Key dependencies include:



```text

openjdk-17-jre-headless

boto3

pyspark

pandas

```



\---



\## Running the Project



\### Prerequisites



Install:



```text

Docker Desktop

AWS CLI

Git

```



You also need:



\- An AWS account

\- An S3 bucket

\- AWS credentials configured locally

\- Olist datasets uploaded to the S3 `raw-data/` prefix



\---



\### Build the Docker Image



Run:



```bash

docker compose build

```



\---



\### Start the Airflow Container



Run:



```bash

docker compose up -d

```



Verify the container:



```bash

docker ps

```



\---



\### Open Apache Airflow



Open:



```text

http://localhost:8080

```



Sign in to the Airflow dashboard.



Find:



```text

olist\_data\_pipeline

```



Enable the DAG and trigger it once.



\---



\## Airflow Execution



The pipeline executes:



```text

download\_raw\_data\_from\_s3

&#x20;         |

&#x20;         v

process\_late\_shipments

&#x20;         |

&#x20;         v

upload\_processed\_data\_to\_s3

```



A successful DAG run shows all three tasks in the \*\*Success\*\* state.



This confirms that the complete automated pipeline executed successfully.



\---



\## Verified Pipeline Result



The pipeline was successfully executed end-to-end.



\### Final Dataset



```text

File:

late\_shipments.csv



Rows:

10,423



Columns:

13

```



\### Final Cloud Location



```text

processed-data/late\_shipments.csv

```



The final file was verified inside AWS S3 after successful Airflow execution.



\---



\## Example Output Columns



The final dataset contains:



```text

order\_id

seller\_id

shipping\_limit\_date

price

freight\_value

product\_id

product\_category\_name

customer\_id

order\_status

order\_purchase\_timestamp

order\_delivered\_carrier\_date

order\_delivered\_customer\_date

order\_estimated\_delivery\_date

```



\---



\## Security



AWS credentials are \*\*not stored in this GitHub repository\*\*.



AWS credentials are configured locally and mounted into the Docker container when required.



Sensitive and local files are excluded using `.gitignore`.



Examples include:



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



AWS access keys and secret access keys should never be committed to a public GitHub repository.



\---



\## Key Data Engineering Concepts Demonstrated



This project demonstrates practical experience with:



\- End-to-end ETL pipeline development

\- Apache Airflow workflow orchestration

\- PySpark data processing

\- Spark SQL transformations

\- Multi-table joins

\- Business-rule-based filtering

\- AWS S3 cloud storage

\- Data lake organization

\- Python and Boto3 AWS integration

\- Docker containerization

\- Pipeline dependency management

\- Raw and processed data separation

\- Automated cloud data movement



\---



\## Dataset



The project uses the \*\*Brazilian E-Commerce Public Dataset by Olist\*\*.



The dataset contains relational e-commerce information covering:



\- Orders

\- Customers

\- Sellers

\- Products

\- Payments

\- Reviews

\- Logistics



The complete raw dataset is not included in this GitHub repository because of its size.



\---



\## Future Improvements



Possible improvements include:



\- Use PostgreSQL instead of SQLite for Airflow metadata

\- Run Spark workloads using AWS EMR

\- Store transformed datasets in Parquet format

\- Partition processed datasets for efficient querying

\- Add automated data-quality checks

\- Add pipeline monitoring and failure alerts

\- Use AWS Glue Data Catalog

\- Query processed data using Amazon Athena

\- Schedule automatic daily pipeline execution

\- Build analytics dashboards from processed datasets



\---



\## Project Outcome



The complete pipeline was successfully implemented and verified:



```text

AWS S3 Raw Data

&#x20;       |

&#x20;       v

Python + Boto3

&#x20;       |

&#x20;       v

Apache Airflow

&#x20;       |

&#x20;       v

PySpark + Spark SQL

&#x20;       |

&#x20;       v

Multi-Table ETL Transformation

&#x20;       |

&#x20;       v

10,423 Late-Shipment Records

&#x20;       |

&#x20;       v

late\_shipments.csv

&#x20;       |

&#x20;       v

AWS S3 Processed Data

```



\*\*Project Status: Completed Successfully\*\*

