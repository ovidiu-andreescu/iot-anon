from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time
import random

# Create a registry to collect metrics
registry = CollectorRegistry()

# Define HDFS-related metrics
rows_metric = Gauge("hdfs_rows_processed", "Number of rows processed", registry=registry)
job_runs_metric = Gauge("hdfs_job_runs", "Number of times the job has run", registry=registry)
job_status_metric = Gauge("hdfs_job_status", "Status of the job (1 for running, 0 for not running)", registry=registry)
disk_space_metric = Gauge("hdfs_disk_space_usage", "Disk space usage percentage", registry=registry)
hadoop_space_metric = Gauge("hadoop_space_used", "Hadoop space used in GB", registry=registry)

# Define Spark-related metrics
spark_nodes_metric = Gauge("spark_nodes_running", "Number of Spark nodes running", registry=registry)

# Define Kafka-related metrics
kafka_messages_metric = Gauge("kafka_messages_sent", "Number of Kafka messages sent", registry=registry)
kafka_brokers_metric = Gauge("kafka_brokers_available", "Number of Kafka brokers available", registry=registry)

# Define Job timing metric
time_taken_metric = Gauge("time_taken_for_job", "Time taken to complete the job in seconds", registry=registry)

# Pushgateway URL
PUSHGATEWAY_URL = "http://localhost:9091"

# Simulated function to generate random HDFS data
def simulate_hdfs_data():
    """
    Simulates HDFS data for metrics.
    """
    rows_processed = random.randint(1000, 10000)  # Random number of rows processed
    job_runs = random.randint(1, 10)             # Random number of job runs
    job_status = random.choice([0, 1])           # Random job status (running or not running)
    disk_space = random.uniform(50, 99)          # Random disk space usage (in %)
    hadoop_space = random.uniform(10, 50)        # Random Hadoop space used (in GB)

    return rows_processed, job_runs, job_status, disk_space, hadoop_space

# Simulated function to generate random Spark data
def simulate_spark_data():
    """
    Simulates Spark data for metrics.
    """
    spark_nodes = random.randint(1, 5)  # Random number of Spark nodes running
    return spark_nodes

# Simulated function to generate random Kafka data
def simulate_kafka_data():
    """
    Simulates Kafka data for metrics.
    """
    kafka_messages = random.randint(100, 1000)  # Random number of Kafka messages sent
    kafka_brokers = random.randint(1, 3)       # Random number of Kafka brokers available
    return kafka_messages, kafka_brokers

# Main loop to push metrics
def main():
    while True:
        # Start timer to measure job duration
        start_time = time.time()

        # Simulate data
        rows_processed, job_runs, job_status, disk_space, hadoop_space = simulate_hdfs_data()
        spark_nodes = simulate_spark_data()
        kafka_messages, kafka_brokers = simulate_kafka_data()

        # Simulate time taken for the job
        time_taken = random.uniform(1, 10)  # Random time taken for job in seconds
        time.sleep(time_taken)  # Simulate the job running

        # Set the metric values
        rows_metric.set(rows_processed)
        job_runs_metric.set(job_runs)
        job_status_metric.set(job_status)
        disk_space_metric.set(disk_space)
        hadoop_space_metric.set(hadoop_space)
        spark_nodes_metric.set(spark_nodes)
        kafka_messages_metric.set(kafka_messages)
        kafka_brokers_metric.set(kafka_brokers)
        time_taken_metric.set(time_taken)

        # Push metrics to Prometheus Pushgateway
        try:
            push_to_gateway(PUSHGATEWAY_URL, job="hdfs_spark_kafka_metrics", registry=registry)
            elapsed_time = time.time() - start_time
            print(f"Metrics pushed successfully:\n"
                  f"HDFS: Rows={rows_processed}, Job Runs={job_runs}, Job Status={job_status}, Disk Space={disk_space:.2f}%, Hadoop Space={hadoop_space:.2f}GB\n"
                  f"Spark: Nodes Running={spark_nodes}\n"
                  f"Kafka: Messages Sent={kafka_messages}, Brokers Available={kafka_brokers}\n"
                  f"Job Time Taken={time_taken:.2f}s, Total Elapsed Time={elapsed_time:.2f}s")
        except Exception as e:
            print(f"Failed to push metrics: {e}")

        # Wait for the next update
        time.sleep(10)

if __name__ == "__main__":
    main()
