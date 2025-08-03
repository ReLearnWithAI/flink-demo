#!/bin/bash

# Apache Flink Kafka Project Runner
# This script helps you run the Flink Kafka project locally

set -e

echo "🚀 Apache Flink Kafka Project Runner"
echo "====================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Check if Maven is installed
check_maven() {
    if ! command -v mvn &> /dev/null; then
        print_error "Maven is not installed. Please install Maven and try again."
        exit 1
    fi
    print_success "Maven is installed"
}

# Check if Java is installed
check_java() {
    if ! command -v java &> /dev/null; then
        print_error "Java is not installed. Please install Java 11+ and try again."
        exit 1
    fi
    
    JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
    if [ "$JAVA_VERSION" -lt 11 ]; then
        print_error "Java 11 or higher is required. Current version: $JAVA_VERSION"
        exit 1
    fi
    print_success "Java is installed (version: $JAVA_VERSION)"
}

# Start Kafka infrastructure
start_kafka() {
    print_status "Starting Kafka infrastructure..."
    
    if [ ! -f "docker-compose.yml" ]; then
        print_error "docker-compose.yml not found in current directory"
        exit 1
    fi
    
    docker-compose up -d
    
    # Wait for Kafka to be ready
    print_status "Waiting for Kafka to be ready..."
    sleep 10
    
    # Check if Kafka is running
    if docker-compose ps | grep -q "kafka.*Up"; then
        print_success "Kafka infrastructure is running"
    else
        print_error "Failed to start Kafka infrastructure"
        exit 1
    fi
}

# Build the project
build_project() {
    print_status "Building the project..."
    mvn clean compile
    print_success "Project compiled successfully"
}

# Run tests
run_tests() {
    print_status "Running tests..."
    mvn test
    print_success "Tests completed successfully"
}

# Run MiniCluster tests
run_mini_cluster_tests() {
    print_status "Running MiniCluster tests (no external dependencies)..."
    mvn test -Dtest=FlinkMiniClusterTest
    print_success "MiniCluster tests completed successfully"
}

# Create topics
create_topics() {
    print_status "Creating Kafka topics..."
    mvn exec:java -Dexec.mainClass="com.example.flink.util.KafkaUtils" -q
    print_success "Kafka topics created"
}

# Generate test data
generate_data() {
    local count=${1:-50}
    local delay=${2:-1000}
    
    print_status "Generating $count test messages..."
    mvn exec:java -Dexec.mainClass="com.example.flink.util.DataGenerator" \
        -Dexec.args="localhost:9092 input-topic $count $delay" -q
    print_success "Test data generated"
}

# Run the Flink job
run_flink_job() {
    print_status "Starting Flink Kafka streaming job..."
    print_warning "Press Ctrl+C to stop the job"
    
    mvn exec:java -Dexec.mainClass="com.example.flink.KafkaStreamJob"
}

# Show help
show_help() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  setup       - Set up the complete environment (Kafka + build + topics)"
    echo "  start       - Start Kafka infrastructure only"
    echo "  build       - Build the project only"
    echo "  test        - Run tests only"
    echo "  test-mini   - Run MiniCluster tests only (no external dependencies)"
    echo "  topics      - Create Kafka topics only"
    echo "  generate    - Generate test data (default: 50 messages, 1s delay)"
    echo "  run         - Run the Flink job"
    echo "  full        - Complete setup and run (setup + generate + run)"
    echo "  stop        - Stop Kafka infrastructure"
    echo "  clean       - Clean up everything (stop containers + clean build)"
    echo "  help        - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 setup     # Set up everything"
    echo "  $0 full      # Set up and run the complete pipeline"
    echo "  $0 test-mini # Run MiniCluster tests (no Kafka needed)"
    echo "  $0 generate  # Generate test data"
    echo "  $0 run       # Run the Flink job"
}

# Main script logic
case "${1:-help}" in
    "setup")
        check_docker
        check_maven
        check_java
        start_kafka
        build_project
        run_tests
        create_topics
        print_success "Setup completed successfully!"
        print_status "You can now run: $0 generate or $0 run"
        ;;
    "start")
        check_docker
        start_kafka
        ;;
    "build")
        check_maven
        check_java
        build_project
        ;;
    "test")
        check_maven
        check_java
        run_tests
        ;;
    "test-mini")
        check_maven
        check_java
        run_mini_cluster_tests
        ;;
    "topics")
        check_maven
        create_topics
        ;;
    "generate")
        check_maven
        generate_data "${2:-50}" "${3:-1000}"
        ;;
    "run")
        check_maven
        check_java
        run_flink_job
        ;;
    "full")
        check_docker
        check_maven
        check_java
        start_kafka
        build_project
        run_tests
        create_topics
        generate_data 50 1000
        run_flink_job
        ;;
    "stop")
        print_status "Stopping Kafka infrastructure..."
        docker-compose down
        print_success "Kafka infrastructure stopped"
        ;;
    "clean")
        print_status "Cleaning up..."
        docker-compose down -v
        mvn clean
        print_success "Cleanup completed"
        ;;
    "help"|*)
        show_help
        ;;
esac 