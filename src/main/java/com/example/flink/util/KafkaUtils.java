package com.example.flink.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Utility class for Kafka operations
 */
public class KafkaUtils {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    
    /**
     * Creates Kafka topics if they don't exist
     */
    public static void createTopics(String bootstrapServers, String... topicNames) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        
        try (AdminClient adminClient = AdminClient.create(props)) {
            for (String topicName : topicNames) {
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                LOG.info("Created topic: {}", topicName);
            }
        } catch (ExecutionException | InterruptedException e) {
            LOG.warn("Error creating topics: {}", e.getMessage());
        }
    }
    
    /**
     * Creates a Kafka producer
     */
    public static KafkaProducer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        return new KafkaProducer<>(props);
    }
    
    /**
     * Sends a message to a Kafka topic
     */
    public static void sendMessage(KafkaProducer<String, String> producer, String topic, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOG.error("Error sending message to topic {}: {}", topic, exception.getMessage());
            } else {
                LOG.debug("Message sent to topic {} partition {} offset {}", 
                    metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }
    
    /**
     * Sends a message to a Kafka topic without a key
     */
    public static void sendMessage(KafkaProducer<String, String> producer, String topic, String message) {
        sendMessage(producer, topic, null, message);
    }
    
    /**
     * Closes a Kafka producer
     */
    public static void closeProducer(KafkaProducer<String, String> producer) {
        if (producer != null) {
            producer.flush();
            producer.close();
            LOG.info("Kafka producer closed");
        }
    }
    
    /**
     * Creates default topics for the Flink Kafka project
     */
    public static void createDefaultTopics() {
        createTopics(DEFAULT_BOOTSTRAP_SERVERS, 
            "input-topic", 
            "output-topic", 
            "aggregated-topic");
    }
} 