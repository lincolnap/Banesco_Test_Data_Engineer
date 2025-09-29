FROM apache/airflow:2.9.3

# Switch to root user to install system packages
USER root

# Install Java 17, curl, wget and Spark client (required for SparkSubmitOperator)
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk \
        curl \
        wget \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark client (lightweight, compatible with cluster version 3.5.6)
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.6-bin-hadoop3.tgz && \
    mv spark-3.5.6-bin-hadoop3 /opt/spark && \
    rm spark-3.5.6-bin-hadoop3.tgz

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Create symlink for compatibility
RUN ln -sf /usr/lib/jvm/java-17-openjdk-arm64 /usr/lib/jvm/java-17-openjdk-amd64

# Copy Spark configuration
COPY stack/spark/config/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

# Verify installations
RUN java -version && echo "Java installation verified" && \
    $SPARK_HOME/bin/spark-submit --version && echo "Spark client installation verified"

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY stack/airflow/requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /opt/airflow

# Copy scripts directory
COPY stack/airflow/scripts/ /opt/airflow/scripts/

# Copy setup parameters
COPY setup_parameters/ /opt/airflow/setup_parameters/

# S3A JARs will be downloaded automatically by Spark using packages configuration

# Set the entrypoint
ENTRYPOINT ["/opt/airflow/setup_parameters/entrypoint.sh"]

# Default command
CMD ["scheduler"]
