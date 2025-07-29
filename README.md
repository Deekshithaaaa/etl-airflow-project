# ETL Airflow Project

## Project Overview
This project contains Apache Airflow ETL pipelines designed to automate data ingestion from various API endpoints into PostgreSQL staging tables. It enables efficient, scheduled extraction, transformation, and loading of data, ensuring data availability for downstream analytics and reporting.

## Setup Instructions

### Prerequisites
- Python 3.7 or higher
- Apache Airflow installed and configured
- PostgreSQL database accessible for staging tables

### Installation Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/Deekshithaaaa/etl-airflow-project.git
   cd etl-airflow-project

2. (Optional) Create and activate a virtual environment:
python3 -m venv venv
source venv/bin/activate

3. Install dependencies:
pip install -r requirements.txt

4. Configure Airflow connections for PostgreSQL and APIs using the Airflow UI or CLI:
Set up a connection named postgres_default for the Postgres database.
Set up any API connections required, e.g., with basic authentication credentials.

## DAG Descriptions

| DAG Filename        | Description                               | API Endpoint Covered |
| ------------------- | ----------------------------------------- | -------------------- |
| `orders_dag.py`     | Extracts and loads orders data            | `/orders/`           |
| `payments_dag.py`   | Extracts and loads payments data          | `/payments/`         |
| `products_dag.py`   | Extracts and loads product information    | `/products/`         |
| `users_dag.py`      | Extracts and loads user data              | `/users/`            |
| `sellers_dag.py`    | Extracts and loads sellers data           | `/sellers/`          |
| `feedback_dag.py`   | Extracts and loads customer feedback data | `/feedback/`         |
| `kafka_test_dag.py` | Test DAG for Kafka messaging              | N/A                  |

Each DAG performs the following:
- Fetches data from the respective API endpoint using HTTP requests.
- Loads the extracted data into corresponding PostgreSQL staging tables.
- Logs execution and errors for monitoring.

## Usage
# Running DAGs
- Trigger DAGs manually from the Airflow UI by selecting the DAG and clicking the Trigger DAG button.
- Schedule DAGs to run automatically as configured in their schedule interval.
# Monitoring
- Monitor DAG run statuses, task progress, and logs from the Airflow webserver UI.
- Check logs for troubleshooting failed tasks.

## Notes
- Ensure the Airflow environment has network access to the APIs and PostgreSQL instance.
- Make sure PostgreSQL tables exist or are created before running DAGs.
- For authentication, use Airflow connection configurations with appropriate credentials.
- Adjust DAG schedule intervals and retries as per your project requirements.
- This project uses basic HTTP authentication for API access.
- Contact the project maintainer for questions or support.

---

*Last updated: July 2025*
