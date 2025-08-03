# Apache Flink Kafka Project

A complete Apache Flink project with Kafka integration for real-time stream processing. This project demonstrates reading from Kafka topics, processing JSON messages, and writing results back to Kafka topics.

## Features

- **Real-time Stream Processing**: Process JSON messages from Kafka in real-time
- **Message Enrichment**: Add processing timestamps and metadata to messages
- **Window Aggregations**: Aggregate messages in time-based windows
- **Fault Tolerance**: Checkpointing enabled for fault tolerance
- **Comprehensive Testing**: Unit tests and integration tests with TestContainers
- **Local Development**: Docker Compose setup for Kafka and Zookeeper

## Project Structure

```
flink-kafka-project/
├── src/
│   ├── main/
│   │   ├── java/com/example/flink/
│   │   │   ├── KafkaStreamJob.java          # Main Flink streaming job
│   │   │   ├── model/
│   │   │   │   └── Event.java               # Data model
│   │   │   └── util/
│   │   │       ├── KafkaUtils.java          # Kafka utility functions
│   │   │       └── DataGenerator.java       # Test data generator
│   │   └── resources/
│   │       └── logback.xml                  # Logging configuration
│   └── test/
│       └── java/com/example/flink/
│           ├── KafkaStreamJobTest.java      # Unit tests
│           └── KafkaIntegrationTest.java    # Integration tests
├── pom.xml                                  # Maven configuration
├── docker-compose.yml                       # Local Kafka setup
└── README.md                                # This file
```

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Docker and Docker Compose (for local Kafka setup)

## Quick Start

### 1. Start Kafka Infrastructure

```bash
# Start Kafka, Zookeeper, and Kafka UI
docker-compose up -d

# Verify services are running
docker-compose ps
```

Kafka will be available at `localhost:9092`
Kafka UI will be available at `http://localhost:8080`

### 2. Build the Project

```bash
# Clean and compile
mvn clean compile

# Run tests
mvn test

# Package the application
mvn package
```

### 3. Generate Test Data

```bash
# Generate 100 test messages with 1-second delay
mvn exec:java -Dexec.mainClass="com.example.flink.util.DataGenerator" \
    -Dexec.args="localhost:9092 input-topic 100 1000"
```

### 4. Run the Flink Job

#### Option A: Run Locally (Recommended for Development)

```bash
# Run the main Flink job
mvn exec:java -Dexec.mainClass="com.example.flink.KafkaStreamJob"
```

#### Option B: Submit to Flink Cluster

```bash
# Build fat JAR
mvn clean package -Pflink-run

# Submit to Flink cluster
flink run target/flink-kafka-project-1.0.0.jar
```

### 5. Monitor the Results

- **Kafka UI**: Visit `http://localhost:8080` to monitor topics and messages
- **Logs**: Check the console output and `logs/flink-kafka.log` file
- **Topics**: Monitor `input-topic`, `output-topic`, and `aggregated-topic`

## Configuration

### Kafka Configuration

The application uses the following default Kafka configuration:

- **Bootstrap Servers**: `localhost:9092`
- **Input Topic**: `input-topic`
- **Output Topic**: `output-topic`
- **Aggregated Topic**: `aggregated-topic`
- **Consumer Group**: `flink-kafka-group`

### Flink Configuration

- **Checkpointing**: Enabled every 10 seconds
- **Parallelism**: Configurable via environment
- **Window Size**: 30 seconds for aggregations

## Data Flow

1. **Input**: JSON messages are sent to `input-topic`
2. **Processing**: Flink processes messages through multiple stages:
   - JSON parsing and validation
   - Message enrichment with timestamps
   - Window-based aggregations
3. **Output**: Processed messages are sent to:
   - `output-topic`: Individual processed messages
   - `aggregated-topic`: Window aggregation results

## Message Format

### Input Message Example
```json
{
  "id": "uuid-123",
  "type": "user_login",
  "user_id": "user1",
  "source": "web",
  "timestamp": 1640995200000,
  "session_id": "session-456",
  "ip_address": "192.168.1.1",
  "user_agent": "Mozilla/5.0..."
}
```

### Output Message Example
```json
{
  "id": "uuid-123",
  "type": "user_login",
  "user_id": "user1",
  "source": "web",
  "timestamp": 1640995200000,
  "session_id": "session-456",
  "ip_address": "192.168.1.1",
  "user_agent": "Mozilla/5.0...",
  "processed_at": 1640995201000,
  "processor": "flink-kafka-job",
  "enriched": true,
  "enrichment_timestamp": 1640995201500
}
```

### Aggregated Message Example
```json
{
  "count": 15,
  "window_start": 1640995200000,
  "last_message_timestamp": 1640995230000,
  "aggregation_timestamp": 1640995230500
}
```

## Testing

### Unit Tests

```bash
# Run unit tests
mvn test -Dtest=KafkaStreamJobTest
```

### MiniCluster Tests (Recommended for Local Testing)

```bash
# Run MiniCluster tests (no external dependencies required)
mvn test -Dtest=FlinkMiniClusterTest

# Or use the convenience script
./run.sh test-mini
```

The MiniCluster tests use Flink's embedded cluster to test the complete streaming pipeline without requiring Kafka or external dependencies. This is perfect for:
- Local development and testing
- CI/CD pipelines
- Quick validation of streaming logic
- Testing without infrastructure setup

### Integration Tests

```bash
# Run integration tests (requires Docker)
mvn test -Dtest=KafkaIntegrationTest
```

### Manual Testing

1. Start the infrastructure: `docker-compose up -d`
2. Run the Flink job: `mvn exec:java -Dexec.mainClass="com.example.flink.KafkaStreamJob"`
3. Generate test data: `mvn exec:java -Dexec.mainClass="com.example.flink.util.DataGenerator"`
4. Monitor results in Kafka UI

## Development

### Adding New Processors

1. Create a new class implementing `MapFunction<String, String>`
2. Add it to the processing pipeline in `KafkaStreamJob.java`
3. Add corresponding tests

### Customizing Configuration

Modify the constants in `KafkaStreamJob.java`:
```java
private static final String KAFKA_BROKERS = "localhost:9092";
private static final String INPUT_TOPIC = "input-topic";
private static final String OUTPUT_TOPIC = "output-topic";
```

### Adding New Data Types

1. Extend the `Event` model in `src/main/java/com/example/flink/model/Event.java`
2. Update the `DataGenerator` to create new data types
3. Add processing logic in the main job

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**
   - Ensure Docker Compose services are running: `docker-compose ps`
   - Check Kafka is accessible: `telnet localhost 9092`

2. **Topic Not Found**
   - Topics are auto-created, but you can manually create them:
   ```bash
   mvn exec:java -Dexec.mainClass="com.example.flink.util.KafkaUtils"
   ```

3. **Flink Job Fails**
   - Check logs in `logs/flink-kafka.log`
   - Verify Kafka connectivity
   - Ensure sufficient memory for Flink

4. **Test Failures**
   - Ensure Docker is running for integration tests
   - Check TestContainers can pull Kafka image
   - Increase timeouts if tests are slow

### Logs

- **Application Logs**: `logs/flink-kafka.log`
- **Kafka Logs**: `docker-compose logs kafka`
- **Zookeeper Logs**: `docker-compose logs zookeeper`

## Performance Tuning

### Flink Configuration

- **Parallelism**: Adjust based on available cores
- **Checkpointing**: Tune interval based on fault tolerance needs
- **Memory**: Configure heap size for large datasets

### Kafka Configuration

- **Partitions**: Increase for better parallelism
- **Replication**: Configure for production environments
- **Retention**: Set appropriate message retention policies

## Production Deployment

### Prerequisites

- Apache Flink cluster
- Apache Kafka cluster
- Monitoring and alerting setup

### Deployment Steps

1. Build the fat JAR: `mvn clean package -Pflink-run`
2. Upload JAR to Flink cluster
3. Submit job with appropriate configuration
4. Monitor job status and metrics

### Configuration for Production

- Use external Kafka cluster
- Configure proper security (SASL/SSL)
- Set up monitoring and alerting
- Configure proper checkpointing storage
- Use production-grade logging

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs
3. Create an issue with detailed information 