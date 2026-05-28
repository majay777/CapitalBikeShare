# Capital Bike Share Analytics - Setup and Run Guide

This guide provides step-by-step instructions to recreate and run the Capital Bike Share Analytics project with Airflow, Kafka, Spark, and MinIO.

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM and 4 CPUs available
- Git installed
- Python 3.10+ (for local development)

## Project Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose Stack                      │
├─────────────────────────────────────────────────────────────┤
│  Airflow (Scheduler, Worker, Webserver)                     │
│  ├── DAGs: Batch Processing, Streaming, Ingestion            │
│  └── Tasks: Spark Jobs, Kafka Producers                      │
├─────────────────────────────────────────────────────────────┤
│  Apache Spark (Master + Workers)                             │
│  ├── Batch Analytics                                         │
│  ├── Streaming Processing                                    │
│  └── Delta Lake Storage                                      │
├─────────────────────────────────────────────────────────────┤
│  Apache Kafka + Zookeeper                                    │
│  ├── Real-time Data Streaming                               │
│  └── Topic: bike_station_status                              │
├─────────────────────────────────────────────────────────────┤
│  MinIO (S3-compatible Storage)                               │
│  ├── Trip Data (Parquet/Delta)                               │
│  └── Analytics Results                                       │
├─────────────────────────────────────────────────────────────┤
│  PostgreSQL (Airflow Metadata)                               │
│  Redis (Celery Broker)                                        │
│  Streamlit (Dashboard)                                       │
└─────────────────────────────────────────────────────────────┘
```

## Step 1: Clone and Setup Project

```bash
# Clone the repository
git clone <repository-url>
cd pyspark-projects

# Create necessary directories
mkdir -p spark-apps
mkdir -p kafka-data zookeeper-data zookeeper-log minio-data
mkdir -p logs plugins config
```

## Step 2: Configure Environment Variables

Create `.env` file in the project root:

```bash
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
_PIP_ADDITIONAL_REQUIREMENTS=s3fs kafka-python boto3 pyarrow deltalake
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
```

## Step 3: Build and Start Docker Services

```bash
# Build custom Airflow image (if needed)
docker-compose build

# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f spark-master
docker-compose logs -f kafka
```

## Step 4: Initialize MinIO and Create Buckets

```bash
# Access MinIO console at http://localhost:9001
# Username: MIN
# Password: minioadmin

# Or use MinIO client (mc)
docker exec -it minio-spark mc alias set local http://localhost:9000 MIN minioadmin
docker exec -it minio-spark mc mb local/tripdata
docker exec -it minio-spark mc mb local/delta
docker exec -it minio-spark mc mb local/bike-data
```

## Step 5: Setup Airflow

```bash
# Wait for Airflow to initialize (check logs)
docker-compose logs -f airflow-init

# Access Airflow Web UI
# URL: http://localhost:8080
# Username: airflow
# Password: airflow

# Configure Airflow Connections
# 1. Go to Admin > Connections
# 2. Create/update connections:
#    - spark_default: spark://spark-master:7077
#    - kafka_default: kafka:9092
#    - minio_default: http://minio:9000
```

## Step 6: Upload Data to MinIO

```bash
# Upload trip data (parquet format)
docker exec -it minio-spark mc cp /local/path/to/tripdata local/tripdata/

# Organize by year
docker exec -it minio-spark mc cp /local/path/to/2020/ local/tripdata/parquet/trips/year=2020/
docker exec -it minio-spark mc cp /local/path/to/2021/ local/tripdata/parquet/trips/year=2021/
```

## Step 7: Start Streamlit Dashboard

```bash
# Streamlit is already configured in docker-compose
# Access at: http://localhost:8501

# Or run locally:
cd streamlit
pip install -r requirements.txt
streamlit run advanced/app.py
```

## Running Airflow DAGs

### DAG 1: Capital Bikeshare Monthly Ingestion

**Purpose**: Ingest monthly trip data from external sources

```bash
# Trigger via Airflow UI:
# 1. Go to DAGs > capital_bikeshare_monthly_ingest
# 2. Click "Trigger DAG"
# 3. Monitor task execution in "Grid View"

# Or trigger via CLI:
docker exec -it airflow-scheduler airflow dags trigger capital_bikeshare_monthly_ingest
```

**Configuration**: Update `dags/capital_bikeshare_monthly_ingest.py` with your data source URLs.

### DAG 2: Bike Share Batch Analytics

**Purpose**: Hourly analytics on station data

```bash
# Trigger via Airflow UI:
# 1. Go to DAGs > bike_share_batch_analytics
# 2. Click "Trigger DAG"
# 3. Monitor execution

# Or trigger via CLI:
docker exec -it airflow-scheduler airflow dags trigger bike_share_batch_analytics
```

**What it does**:
- Calculates station utilization percentages
- Analyzes peak hours
- Detects outages (stations with 0 bikes)
- Saves results to Delta Lake in MinIO

### DAG 3: Bike Share Streaming Pipeline

**Purpose**: Real-time streaming from Kafka to Spark

```bash
# This DAG is set to manual trigger
# 1. Ensure Kafka producer is running
# 2. Go to DAGs > bike_share_streaming_pipeline
# 3. Click "Trigger DAG"

# Or trigger via CLI:
docker exec -it airflow-scheduler airflow dags trigger bike_share_streaming_pipeline
```

**Prerequisites**:
- Kafka topic `bike_station_status` must exist
- Kafka producer must be running

### DAG 4: Kafka Producer

**Purpose**: Fetch real-time station data and publish to Kafka

```bash
# Scheduled to run every 2 minutes
# Monitor in Airflow UI

# Manual trigger:
docker exec -it airflow-scheduler airflow dags trigger kafka_producer_dag
```

**What it does**:
- Fetches station status from GBFS API
- Publishes to Kafka topic `bike_station_status`
- Runs every 2 minutes by default

### DAG 5: Bay Batch Spark Submit

**Purpose**: Submit Spark jobs for Bay Area bike analytics

```bash
# Currently commented out - uncomment in dags/bay_batch_spark_submit.py
# Configure Spark application path
# Trigger via Airflow UI
```

## Monitoring and Debugging

### Check Airflow Logs

```bash
# Scheduler logs
docker-compose logs -f airflow-scheduler

# Worker logs
docker-compose logs -f airflow-worker

# Specific DAG run
docker exec -it airflow-scheduler airflow dags list
docker exec -it airflow-scheduler airflow dags list-runs -d bike_share_batch_analytics
```

### Check Spark Jobs

```bash
# Spark Master UI: http://localhost:8080
# Spark Worker UI: http://localhost:8081

# View Spark logs
docker-compose logs -f spark-master
docker-compose logs -f spark-worker
```

### Check Kafka

```bash
# Kafka UI: http://localhost:8088

# List topics
docker exec -it kafka-spark kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker exec -it kafka-spark kafka-console-consumer --bootstrap-server localhost:9092 --topic bike_station_status --from-beginning
```

### Check MinIO

```bash
# MinIO Console: http://localhost:9001
# Username: MIN
# Password: minioadmin

# List buckets
docker exec -it minio-spark mc ls local/

# Check data
docker exec -it minio-spark mc ls local/tripdata/
```

## Common Issues and Solutions

### Issue: Airflow DAGs not appearing

**Solution**:
```bash
# Check DAG folder mounting
docker exec -it airflow-scheduler ls -la /opt/airflow/dags/

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Issue: Spark connection refused

**Solution**:
```bash
# Check Spark master is running
docker-compose ps spark-master

# Check Spark logs
docker-compose logs spark-master

# Verify connection string in Airflow: spark://spark-master:7077
```

### Issue: Kafka connection timeout

**Solution**:
```bash
# Check Kafka is healthy
docker-compose ps kafka

# Wait for Kafka to fully start (can take 30-60 seconds)
docker-compose logs -f kafka

# Verify topic exists
docker exec -it kafka-spark kafka-topics --list --bootstrap-server localhost:9092
```

### Issue: MinIO authentication failed

**Solution**:
```bash
# Verify credentials match between docker-compose.yaml and DAGs
# docker-compose.yaml: MIN / minioadmin
# DAGs should use: minioadmin / minioadmin

# Update .env file if needed
```

### Issue: Out of memory errors

**Solution**:
```bash
# Increase Docker memory allocation in Docker Desktop settings
# Reduce Spark executor memory in DAG configurations

# Add to Spark config:
# --executor-memory 2g
# --driver-memory 1g
```

## Stopping the Project

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v

# Stop specific services
docker-compose stop airflow-scheduler
docker-compose stop spark-master
```

## Development Workflow

### Adding New DAGs

```bash
# 1. Create DAG file in dags/ directory
# 2. Test DAG syntax
docker exec -it airflow-scheduler python -m py_compile /opt/airflow/dags/your_dag.py

# 3. DAG should appear in Airflow UI automatically
# 4. Trigger and monitor
```

### Adding Spark Applications

```bash
# 1. Create Spark app in spark-apps/ directory
# 2. Mount volume in docker-compose.yaml
# 3. Test locally first
spark-submit --master local[*] your_app.py

# 4. Deploy via Airflow DAG or submit to cluster
spark-submit --master spark://spark-master:7077 your_app.py
```

### Updating Streamlit Dashboard

```bash
# Changes are hot-reloaded automatically
# Access at http://localhost:8501

# For production changes:
docker-compose restart streamlit
```

## Performance Tuning

### Spark Configuration

Edit DAG Spark configurations for better performance:

```python
# Add to Spark config in DAGs
.config("spark.executor.memory", "4g")
.config("spark.executor.cores", "4")
.config("spark.driver.memory", "2g")
.config("spark.dynamicAllocation.enabled", "true")
.config("spark.dynamicAllocation.maxExecutors", "10")
```

### Airflow Configuration

```bash
# Increase parallelism in airflow.cfg
# Edit: /opt/airflow/config/airflow.cfg
parallelism = 32
dag_concurrency = 16
```

## Security Considerations

⚠️ **Important for Production**:

1. Change default passwords in `.env` file
2. Enable SSL/TLS for all connections
3. Use secrets management (e.g., HashiCorp Vault)
4. Restrict network access with Docker networks
5. Enable authentication for MinIO
6. Use environment variables for all sensitive data
7. Remove hardcoded credentials from code

## Additional Resources

- Airflow Documentation: https://airflow.apache.org/docs/
- Spark Documentation: https://spark.apache.org/docs/
- Kafka Documentation: https://kafka.apache.org/documentation/
- MinIO Documentation: https://min.io/docs/
- Streamlit Documentation: https://docs.streamlit.io/

## Support

For issues specific to this project:
1. Check logs: `docker-compose logs <service-name>`
2. Verify service health: `docker-compose ps`
3. Check Airflow DAG logs in Web UI
4. Review this guide's troubleshooting section
