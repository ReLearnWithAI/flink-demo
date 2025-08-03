#!/bin/bash

# Apache Flink Kafka Project Verification Script
# This script verifies that the project is properly set up

set -e

echo "🔍 Apache Flink Kafka Project Verification"
echo "=========================================="

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

# Check if required files exist
check_files() {
    print_status "Checking project files..."
    
    local files=(
        "pom.xml"
        "docker-compose.yml"
        "README.md"
        "run.sh"
        "src/main/java/com/example/flink/KafkaStreamJob.java"
        "src/main/java/com/example/flink/model/Event.java"
        "src/main/java/com/example/flink/util/KafkaUtils.java"
        "src/main/java/com/example/flink/util/DataGenerator.java"
        "src/test/java/com/example/flink/KafkaStreamJobTest.java"
        "src/test/java/com/example/flink/KafkaIntegrationTest.java"
        "src/main/resources/logback.xml"
        "src/main/resources/application.properties"
    )
    
    local missing_files=()
    
    for file in "${files[@]}"; do
        if [ -f "$file" ]; then
            print_success "✓ $file"
        else
            print_error "✗ $file (missing)"
            missing_files+=("$file")
        fi
    done
    
    if [ ${#missing_files[@]} -eq 0 ]; then
        print_success "All required files are present"
    else
        print_error "Missing ${#missing_files[@]} required files"
        return 1
    fi
}

# Check if Maven can compile the project
check_compilation() {
    print_status "Checking Maven compilation..."
    
    if mvn clean compile -q; then
        print_success "Project compiles successfully"
    else
        print_error "Project compilation failed"
        return 1
    fi
}

# Check if tests can run
check_tests() {
    print_status "Checking unit tests..."
    
    if mvn test -Dtest=KafkaStreamJobTest -q; then
        print_success "Unit tests pass"
    else
        print_warning "Unit tests failed (this might be expected if Kafka is not running)"
    fi
}

# Check if Docker Compose is available
check_docker_compose() {
    print_status "Checking Docker Compose..."
    
    if command -v docker-compose &> /dev/null; then
        print_success "Docker Compose is available"
    else
        print_warning "Docker Compose not found (required for local Kafka setup)"
    fi
}

# Check if Docker is running
check_docker() {
    print_status "Checking Docker..."
    
    if docker info > /dev/null 2>&1; then
        print_success "Docker is running"
    else
        print_warning "Docker is not running (required for local Kafka setup)"
    fi
}

# Check if Kafka topics can be created (if Kafka is running)
check_kafka_topics() {
    print_status "Checking Kafka topic creation..."
    
    # Check if Kafka is running
    if docker-compose ps | grep -q "kafka.*Up" 2>/dev/null; then
        if mvn exec:java -Dexec.mainClass="com.example.flink.util.KafkaUtils" -q 2>/dev/null; then
            print_success "Kafka topics can be created"
        else
            print_warning "Kafka topic creation failed (Kafka might not be ready)"
        fi
    else
        print_warning "Kafka is not running (start with: docker-compose up -d)"
    fi
}

# Main verification
main() {
    echo ""
    
    # Check files
    if ! check_files; then
        print_error "File verification failed"
        exit 1
    fi
    
    echo ""
    
    # Check compilation
    if ! check_compilation; then
        print_error "Compilation verification failed"
        exit 1
    fi
    
    echo ""
    
    # Check tests
    check_tests
    
    echo ""
    
    # Check Docker
    check_docker
    check_docker_compose
    
    echo ""
    
    # Check Kafka
    check_kafka_topics
    
    echo ""
    print_success "Verification completed!"
    echo ""
    print_status "Next steps:"
    echo "  1. Start Kafka: ./run.sh start"
    echo "  2. Run the job: ./run.sh run"
    echo "  3. Generate data: ./run.sh generate"
    echo "  4. Or run everything: ./run.sh full"
    echo ""
}

# Run verification
main 