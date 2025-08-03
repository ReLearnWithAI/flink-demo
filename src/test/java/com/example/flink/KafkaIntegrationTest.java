package com.example.flink;

import com.example.flink.util.DataGenerator;
import com.example.flink.util.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Flink Kafka streaming using TestContainers
 */
@Testcontainers
public class KafkaIntegrationTest {
    
    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
    );
    
    private KafkaConsumer<String, String> consumer;
    private String bootstrapServers;
    
    @BeforeEach
    public void setUp() {
        bootstrapServers = kafka.getBootstrapServers();
        
        // Create topics
        KafkaUtils.createTopics(bootstrapServers, "input-topic", "output-topic", "aggregated-topic");
        
        // Set up consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        consumer = new KafkaConsumer<>(props);
    }
    
    @AfterEach
    public void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }
    
    @Test
    public void testEndToEndPipeline() throws Exception {
        // Subscribe to output topic
        consumer.subscribe(Collections.singletonList("output-topic"));
        
        // Generate test messages
        int messageCount = 10;
        DataGenerator.generateMessages(bootstrapServers, "input-topic", messageCount, 100);
        
        // Wait for messages to be processed
        Thread.sleep(5000);
        
        // Consume messages from output topic
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        // Verify that messages were processed
        assertTrue(records.count() > 0, "Should have received processed messages");
        
        // Verify message structure
        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            assertNotNull(message);
            assertTrue(message.contains("processed_at"));
            assertTrue(message.contains("enriched"));
        }
    }
    
    @Test
    public void testMessageProcessingWithRealKafka() throws Exception {
        // Subscribe to output topic
        consumer.subscribe(Collections.singletonList("output-topic"));
        
        // Generate a single test message
        String testMessage = DataGenerator.generateEventMessage();
        
        // Send message to input topic
        KafkaUtils.sendMessage(
                KafkaUtils.createProducer(bootstrapServers),
                "input-topic",
                testMessage
        );
        
        // Wait for processing
        Thread.sleep(3000);
        
        // Consume processed message
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        
        assertEquals(1, records.count(), "Should have received exactly one processed message");
        
        ConsumerRecord<String, String> record = records.iterator().next();
        String processedMessage = record.value();
        
        // Verify the message was processed correctly
        assertTrue(processedMessage.contains("processed_at"));
        assertTrue(processedMessage.contains("enriched"));
        assertTrue(processedMessage.contains("enrichment_timestamp"));
    }
    
    @Test
    public void testAggregatedTopic() throws Exception {
        // Subscribe to aggregated topic
        consumer.subscribe(Collections.singletonList("aggregated-topic"));
        
        // Generate multiple messages to trigger aggregation
        int messageCount = 5;
        DataGenerator.generateMessages(bootstrapServers, "input-topic", messageCount, 500);
        
        // Wait for window aggregation (30-second windows)
        Thread.sleep(35000);
        
        // Consume aggregated messages
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        // Should have at least one aggregated message
        assertTrue(records.count() > 0, "Should have received aggregated messages");
        
        // Verify aggregation structure
        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            assertTrue(message.contains("count"));
            assertTrue(message.contains("window_start"));
            assertTrue(message.contains("aggregation_timestamp"));
        }
    }
    
    @Test
    public void testConcurrentMessageProcessing() throws Exception {
        // Subscribe to output topic
        consumer.subscribe(Collections.singletonList("output-topic"));
        
        // Generate messages concurrently
        int messageCount = 20;
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger processedCount = new AtomicInteger(0);
        
        // Start multiple threads to send messages
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            new Thread(() -> {
                try {
                    String message = DataGenerator.generateEventMessage();
                    KafkaUtils.sendMessage(
                            KafkaUtils.createProducer(bootstrapServers),
                            "input-topic",
                            message
                    );
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        
        // Wait for all messages to be sent
        assertTrue(latch.await(30, TimeUnit.SECONDS), "All messages should be sent within timeout");
        
        // Wait for processing
        Thread.sleep(10000);
        
        // Consume processed messages
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        // Should have processed messages
        assertTrue(records.count() > 0, "Should have received processed messages");
        
        // Verify all messages were processed correctly
        for (ConsumerRecord<String, String> record : records) {
            String message = record.value();
            assertNotNull(message);
            assertTrue(message.contains("processed_at"));
            assertTrue(message.contains("enriched"));
        }
    }
} 