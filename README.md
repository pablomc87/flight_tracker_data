# Flight Data Analysis Project

## Overview

This project is designed to analyze flight data using various tools and technologies. The project includes data ingestion, processing, and analysis of flight information from different sources. The main components of the project are:

- **SQL Scripts**: For creating and managing database tables and performing data analysis.
- **Python Scripts**: For interacting with the FlightRadar24 API to fetch flight data.
- **Kafka**: For the retrieval of flight data in a streaming manner.
- **Airflow DAGs**: For orchestrating data workflows and automating tasks.

They interoperate as follows:
- Kafka streams flight data from the FlightRadar24 API and sends it to PostgreSQL.
- Airflow DAGs manage the data ingestion process, including fetching airport and airlines data, processing it, and storing it in the database. It also includes the creation of silver and gold zone tables for their use in our dashboards.
- Metabase is used for data visualization and dashboard creation, sometimes with SQL queries or with metabase's own functionalities.

## Project Structure

- `data_ingestion/kafka/`: Contains Kafka configuration files for streaming flight data.
- `data_ingestion/airflow/dags/`: Contains Airflow DAGs for orchestrating data workflows.
- `data_ingestion/airflow/dags/sql/`: Contains SQL scripts for creating tables and performing data analysis.
- `data_ingestion/airflow/dags/resources/`: Contains resources used by the Airflow DAGs and some extra data added to the project.
- `data_storage/`: Contains scripts for backing up and restoring the Metabase database, and also for the setup of the postgresql database.

## Project architecture

![Project architecture](https://github.com/user-attachments/assets/89bb8c1c-b0e1-42cf-9c7b-be1a3ee1968c)


## Database Schema (tables of interest)

![Database schema](https://github.com/user-attachments/assets/8b5db099-1573-43d5-a60a-d76d73dd00cc)


## Prerequisites

- Python 3.12
- PostgreSQL
- Apache Airflow
- FlightRadar24 API
- Docker and Docker Compose

## Setup

1. **Clone the repository**:
    ```sh
    git clone git@gitlab.com:exalt-it-dojo/candidats/pablo-calvo-flight-radar-c9ea6ea7-ed6a-4ee2-9e62-905ae0c43e4b.git
    cd pablo-calvo-flight-radar-c9ea6ea7-ed6a-4ee2-9e62-905ae0c43e4b
    ```

2. **Create a virtual environment** (optional):
    ```sh
    python3.12 -m venv .venv
    source .venv/bin/activate
    ```

3. **Install dependencies** (optional):
    ```sh
    pip install -r requirements.txt
    ```

4. **Set up environment variables**:
   - Create a `.env` file in the project root directory.
      You can do so by running the following command:
      ```sh
      echo -e "AIRFLOW_UID=$(id -u)" > .env
      ```
   - Add the following environment variables to the `.env` file:
     ```sh
     POSTGRES_DB=airflow
     POSTGRES_USER=airflow
     POSTGRES_PASSWORD=airflow
     POSTGRES_PORT=5432
     POSTGRES_HOST=postgres
     AIRFLOW_PROJ_DIR=./data_ingestion/airflow
     MB_DB_DBNAME=metabase
     MB_DB_PORT=5432
     MB_DB_USER=metabase
     MB_DB_PASS=metabase
     ```
   These environment variables should be changed in a production environment, especially the database credentials, but for now the project works using these variables that have been set also in the metabase instance to connect to the postgres database.

## Usage

### Running with Docker Compose

1. **Start the services**:
    ```sh
    docker-compose up --build -d
    ```
   When the setup has ended, the following services should be running:
    - PostgreSQL database for airflow and storing data.
    - Airflow services: webserver, scheduler, triggerer, and worker.
    - Metabase instance with its own database.
    - Kafka service for streaming flight data. It includes a broker, a producer, and a consumer.
    - Redis service for Celery task queue.
   
2. **Access the Airflow web interface**:
    - Open the Airflow web interface at `http://localhost:8080`. It might take some time until it is completely up and running.
    - Log in with user airflow and password airflow. This login process will need to change in production environment.

3. **Trigger DAGs**:
    - You can start all dags.

4. **Run metabase restore dump**:
    - Run the following command to create a backup of the metabase database:
    ```sh
    ./data_storage/restore_metabase.sh
    ```
    - The backup will be stored in the `data_storage/` directory.
    - If the file doesn't run, you might have to make it executable by running:
    ```sh
    sudo chmod +x data_storage/restore_metabase.sh
    ```

5. **Access the Metabase web interface**:
    - Open the Metabase web interface at `http://localhost:3000`.
    - Log the credentials I have provided you in my email.
    - You could go to Admin, Databases, and sync the FlightRadarDB database so the dashboards contain the appropriate data.
    - You can check the dashboard I have created for you.

## Fault Tolerance

In this project, fault tolerance is key to ensuring data flow continuity between the FlightRadar24 API, Kafka, and PostgreSQL. The following mechanisms and recommendations provide resilience against potential failures at various stages of the pipeline.

1. Kafka Fault Tolerance

Although our current implementation doesn’t fully utilize Kafka’s built-in fault-tolerance features, here are key Kafka functionalities that should be configured for a production-ready system:

- Replication: Kafka ensures data availability by replicating topics across multiple brokers. In a production environment, setting an appropriate replication factor (e.g., 3) will ensure data redundancy and availability even if a broker fails.
- Acknowledgments (acks): Producers can be configured to wait for acknowledgments that guarantee the data is successfully written and replicated across brokers. Use acks=all in production to ensure message delivery is confirmed only when it’s written to all in-sync replicas.
- Consumer Group Rebalancing: Kafka allows consumers within a group to take over message processing from another consumer if it fails. This ensures that the pipeline remains operational even when some consumers go offline. Our consumer should be part of a group, ensuring that consumer failure doesn’t stop message processing.

2. Application-Level Fault Tolerance

- Retries with Backoff: The producer implements retry logic with exponential backoff for fetching data from the FlightRadar24 API. If the API request fails (e.g., due to rate-limiting or temporary unavailability), the system waits and retries multiple times before giving up. This ensures that transient issues do not disrupt the data pipeline.
```python
retries = 5
backoff_factor = 2
```
We could modify our retry logic to include a more sophisticated backoff strategy, or just giving up for a specific flight. This would prevent the system from getting stuck in a loop trying to fetch a flight that is not available.

- Consumer Polling and Retrying: The Kafka consumer polls for new messages and retries if no messages are found within a set time. If the consumer encounters a message processing error, it will retry until successful or reach the retry limit. This reduces the likelihood of lost or missed data.

```python
attempts = 0
max_attempts = 10
```

### Database Fault Tolerance

- Transaction Management: Inserts into PostgreSQL are wrapped in transactions. If an error occurs during data insertion, the transaction is rolled back, ensuring data consistency and preventing partial writes.

### Airflow Fault Tolerance
- DAG Retries: Airflow DAGs are configured to run regularly, more often than needed by the project. We haven't set up any retry mechanism because we feel that it is pretty robust, but it could be added if needed. With appropriate hooks and operators, we could configure Airflow alerts to notify administrators if a task failed (`SlackNotifier`, `airflow.utils.email.send_email_smtp`).

### Logging and Monitoring
- Logging: Comprehensive logging is in place for both producers and consumers, including message errors, and retries. Delivery reports weren't implemented because it made it harder to read the logs. These logs can help diagnose issues quickly and provide insight into the system’s operational health. Right now they're accessible via the `docker compose logs` command.
In a production environment, it would be appropriate to integrate with centralized logging platforms (e.g., ELK, Datadog) for easier monitoring and analysis.

By combining Kafka's fault-tolerance mechanisms (such as replication and consumer rebalancing) with robust retry logic in the producer and consumer, our pipeline is designed to gracefully handle failures.

## Production Deployment

### Why Not Deployed in Production

The project is not deployed in production mainly due to cost considerations. Running a production-grade setup with continuous data ingestion, processing, and storage can incur significant costs, especially with cloud services.

### Steps for Production Deployment

1. **Cloud Infrastructure**: 
   1. Set up a GCP project.
   2. Deploy Airflow on Google Cloud Composer.
   3. You could use Google Cloud SQL for PostgreSQL for the airflow and metabase databases. You could also have an additional database for the data, if the current structure doesn't fit your requirements.
   4. Use Google Cloud Managed Service for Apache Kafka for streaming flight data. It will allow to replicate the current setup with several brokers, producers, and consumers for fault-tolerance and scalability.
   5. Use Google Cloud Storage for storing data. It will also be used to store dag files.
   6. Use a virtual machine for running the Metabase instance. You could use a marketplace instance too.
   You could streamline the creation of this infrastructure using Terraform.
   A Cloud infrastructure would allow for more scalability and reliability, also ensuring for more control over the environment and the permissions.

#### Suggested Architecture

![Suggested Architecture](https://github.com/user-attachments/assets/8fbc1d4c-90c2-4d46-91bd-6a1877cbdf8f)


### Next steps

1. **CI/CD Pipeline**:
   1. Set up a CI/CD pipeline using Jenkins, GitLab CI/CD, or GitHub Actions.
   2. The pipeline should include steps for linting, testing, building, and deploying the project.
   3. The pipeline should be triggered on every push to the main branch.
   4. More reduced, focused pipelines could be created for development branches.
   5. The pipeline should include steps for deploying the project to the production environment, or you could do that locally with Terraform.

2. **Comprehensive testing**:
   1. Implement unit tests, integration tests, and end-to-end tests for the project.
   2. Use tools like Pytest, KafkaUnit, and Postman for testing

3. **Security**:
   1. Implement security best practices for the project, including encryption, access control, and data protection.
   2. Use secrets management tools like HashiCorp Vault or AWS Secrets Manager to store sensitive information.
   3. Implement role-based access control (RBAC) for Airflow and Metabase.

4. **Monitoring and Alerting**:
   1. Set up monitoring and alerting for the project using tools like Prometheus, Grafana, and ELK Stack.
   2. Monitor key metrics such as data ingestion rate, latency, and error rates.
   3. Set up alerts for critical issues such as data pipeline failures or database outages.

5. **Improved dashboards**:
    1. Create more detailed and interactive dashboards in Metabase for visualizing flight data.
    2. Include additional metrics, charts, and filters to provide deeper insights into the data.
    3. Use Metabase's dashboard sharing feature to share dashboards with stakeholders.

6. **Data Quality Checks**:
    1. Implement data quality checks in Airflow to ensure the integrity of the data.
    2. Perform checks such as data completeness, schema validation, and data accuracy.
    3. Set up alerts for data quality issues and automate data validation processes.

## Project Choices and Explanations

### Airport Continents Script

A specific script was created to retrieve airport continents because the original dataset did not include this information. This script maps countries to their respective continents, ensuring accurate data analysis.

### Top 3 Aircraft Models per Country

For the query "Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage", an SQL script was written to rank aircraft models by flight count within each country and select the top 3 models. This ensures that the most frequently used aircraft models are identified for each country.

## Acknowledgements

- FlightRadar24 for providing the API for flight data.
- Apache Airflow for workflow orchestration.
- PostgreSQL for database management.
- avcodes for airline codes and information on their countries.
- Exalt for providing me with the opportunity to work on this project.

## Contact

For any questions or feedback, feel free to reach out to me at [pablomc87@gmail.com](mailto:pablomc87@gmail.com)
