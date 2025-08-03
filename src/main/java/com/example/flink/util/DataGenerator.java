package com.example.flink.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Data generator for producing test messages to Kafka topics
 */
public class DataGenerator {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Random random = new Random();
    
    private static final String[] EVENT_TYPES = {"user_login", "user_logout", "page_view", "purchase", "click"};
    private static final String[] SOURCES = {"web", "mobile", "api", "batch"};
    private static final String[] USER_IDS = {"user1", "user2", "user3", "user4", "user5"};
    
    /**
     * Generates a random JSON event message
     */
    public static String generateEventMessage() {
        try {
            ObjectNode event = objectMapper.createObjectNode();
            
            event.put("id", UUID.randomUUID().toString());
            event.put("type", EVENT_TYPES[random.nextInt(EVENT_TYPES.length)]);
            event.put("user_id", USER_IDS[random.nextInt(USER_IDS.length)]);
            event.put("source", SOURCES[random.nextInt(SOURCES.length)]);
            event.put("timestamp", System.currentTimeMillis());
            event.put("session_id", UUID.randomUUID().toString());
            event.put("ip_address", generateRandomIP());
            event.put("user_agent", generateRandomUserAgent());
            
            // Add some random data based on event type
            if ("purchase".equals(event.get("type").asText())) {
                event.put("amount", random.nextDouble() * 1000);
                event.put("currency", "USD");
                event.put("product_id", "prod_" + random.nextInt(1000));
            } else if ("page_view".equals(event.get("type").asText())) {
                event.put("page_url", "/page/" + random.nextInt(100));
                event.put("referrer", "https://example.com");
            }
            
            return event.toString();
        } catch (Exception e) {
            LOG.error("Error generating event message", e);
            return "{}";
        }
    }
    
    /**
     * Generates a random IP address
     */
    private static String generateRandomIP() {
        return random.nextInt(256) + "." + 
               random.nextInt(256) + "." + 
               random.nextInt(256) + "." + 
               random.nextInt(256);
    }
    
    /**
     * Generates a random user agent string
     */
    private static String generateRandomUserAgent() {
        String[] browsers = {"Chrome", "Firefox", "Safari", "Edge"};
        String[] os = {"Windows", "MacOS", "Linux", "Android", "iOS"};
        
        return "Mozilla/5.0 (" + os[random.nextInt(os.length)] + ") " + 
               browsers[random.nextInt(browsers.length)] + "/" + 
               (90 + random.nextInt(20)) + ".0";
    }
    
    /**
     * Continuously generates and sends messages to a Kafka topic
     */
    public static void generateMessages(String bootstrapServers, String topic, int messageCount, long delayMs) {
        KafkaProducer<String, String> producer = KafkaUtils.createProducer(bootstrapServers);
        
        try {
            LOG.info("Starting to generate {} messages to topic: {}", messageCount, topic);
            
            for (int i = 0; i < messageCount; i++) {
                String message = generateEventMessage();
                KafkaUtils.sendMessage(producer, topic, message);
                
                LOG.info("Generated message {}/{}: {}", i + 1, messageCount, message.substring(0, Math.min(100, message.length())) + "...");
                
                if (delayMs > 0) {
                    TimeUnit.MILLISECONDS.sleep(delayMs);
                }
            }
            
            LOG.info("Finished generating {} messages to topic: {}", messageCount, topic);
            
        } catch (InterruptedException e) {
            LOG.error("Message generation interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            KafkaUtils.closeProducer(producer);
        }
    }
    
    /**
     * Main method for standalone data generation
     */
    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "input-topic";
        int messageCount = args.length > 2 ? Integer.parseInt(args[2]) : 100;
        long delayMs = args.length > 3 ? Long.parseLong(args[3]) : 1000;
        
        // Create topics first
        KafkaUtils.createDefaultTopics();
        
        // Generate messages
        generateMessages(bootstrapServers, topic, messageCount, delayMs);
    }
} 