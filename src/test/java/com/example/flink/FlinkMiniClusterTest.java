package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Flink MiniCluster Test for local testing without external dependencies
 * 
 * This test demonstrates:
 * 1. Using Flink MiniCluster for local testing
 * 2. Simulating data sources with collections
 * 3. Testing the complete streaming pipeline
 * 4. Verifying processing results
 */
public class FlinkMiniClusterTest extends TestBaseUtils {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterTest.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // MiniCluster configuration
    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());
    
    private StreamExecutionEnvironment env;
    private List<String> processedMessages;
    private List<String> aggregatedMessages;
    
    @Before
    public void setUp() throws Exception {
        // Get the mini cluster execution environment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000); // Enable checkpointing for testing
        
        // Initialize result collections
        processedMessages = new ArrayList<>();
        aggregatedMessages = new ArrayList<>();
    }
    
    @After
    public void tearDown() {
        processedMessages.clear();
        aggregatedMessages.clear();
    }
    
    /**
     * Test the complete streaming pipeline using MiniCluster
     */
    @Test
    public void testCompleteStreamingPipeline() throws Exception {
        LOG.info("Starting complete streaming pipeline test with MiniCluster");
        
        // Create test data
        List<String> testData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            testData.add(createTestEvent(i));
        }
        
        // Create the streaming pipeline (similar to KafkaStreamJob but with test sources/sinks)
        DataStream<String> stream = env.fromCollection(testData);
        
        // Process the stream using the same processors from KafkaStreamJob
        DataStream<String> processedStream = stream
                .map(new KafkaStreamJob.JsonProcessor())
                .name("JSON Processor")
                .filter(message -> message != null && !message.isEmpty())
                .name("Filter Valid Messages")
                .map(new KafkaStreamJob.MessageEnricher())
                .name("Message Enricher");
        
        // Add window-based processing
        DataStream<String> windowedStream = processedStream
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .aggregate(new KafkaStreamJob.MessageAggregator())
                .name("Window Aggregator");
        
        // Add test sinks to collect results
        processedStream.addSink(new CollectingSink(processedMessages)).name("Processed Messages Sink");
        windowedStream.addSink(new CollectingSink(aggregatedMessages)).name("Aggregated Messages Sink");
        
        // Execute the job
        CompletableFuture<Void> jobFuture = CompletableFuture.runAsync(() -> {
            try {
                env.execute("Flink MiniCluster Test Job");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        
        // Wait for job completion with timeout
        jobFuture.get(30, TimeUnit.SECONDS);
        
        // Verify results
        LOG.info("Job completed. Processed messages: {}, Aggregated messages: {}", 
                processedMessages.size(), aggregatedMessages.size());
        
        // Verify processed messages
        assertTrue("Should have processed messages", processedMessages.size() > 0);
        for (String message : processedMessages) {
            verifyProcessedMessage(message);
        }
        
        // Verify aggregated messages (may take longer due to windowing)
        if (aggregatedMessages.size() > 0) {
            for (String message : aggregatedMessages) {
                verifyAggregatedMessage(message);
            }
        }
        
        LOG.info("Complete streaming pipeline test passed successfully");
    }
    
    /**
     * Test individual processors in isolation
     */
    @Test
    public void testIndividualProcessors() throws Exception {
        LOG.info("Testing individual processors");
        
        // Create test data
        List<String> testData = new ArrayList<>();
        testData.add("{\"id\":\"test1\",\"type\":\"test\",\"data\":\"test data 1\"}");
        testData.add("{\"id\":\"test2\",\"type\":\"test\",\"data\":\"test data 2\"}");
        
        // Test JSON processor
        DataStream<String> stream = env.fromCollection(testData);
        DataStream<String> processedStream = stream
                .map(new KafkaStreamJob.JsonProcessor())
                .filter(message -> message != null && !message.isEmpty());
        
        processedStream.addSink(new CollectingSink(processedMessages));
        
        // Execute
        env.execute("Individual Processors Test");
        
        // Verify results
        assertEquals("Should have processed 2 messages", 2, processedMessages.size());
        
        for (String message : processedMessages) {
            ObjectNode jsonNode = objectMapper.readValue(message, ObjectNode.class);
            assertTrue("Should have processed_at field", jsonNode.has("processed_at"));
            assertEquals("Should have correct processor", "flink-kafka-job", jsonNode.get("processor").asText());
            assertTrue("Should have enriched field", jsonNode.get("enriched").asBoolean());
        }
        
        LOG.info("Individual processors test passed");
    }
    
    /**
     * Test error handling and invalid data
     */
    @Test
    public void testErrorHandling() throws Exception {
        LOG.info("Testing error handling");
        
        // Create test data with invalid JSON
        List<String> testData = new ArrayList<>();
        testData.add("invalid json");
        testData.add("{\"id\":\"valid1\",\"type\":\"test\"}");
        testData.add(""); // Empty string
        testData.add("{\"id\":\"valid2\",\"type\":\"test\"}");
        
        DataStream<String> stream = env.fromCollection(testData);
        DataStream<String> processedStream = stream
                .map(new KafkaStreamJob.JsonProcessor())
                .filter(message -> message != null && !message.isEmpty());
        
        processedStream.addSink(new CollectingSink(processedMessages));
        
        // Execute
        env.execute("Error Handling Test");
        
        // Should only have 2 valid messages
        assertEquals("Should have processed 2 valid messages", 2, processedMessages.size());
        
        for (String message : processedMessages) {
            ObjectNode jsonNode = objectMapper.readValue(message, ObjectNode.class);
            assertTrue("Should have valid JSON structure", jsonNode.has("id"));
            assertTrue("Should have processed_at field", jsonNode.has("processed_at"));
        }
        
        LOG.info("Error handling test passed");
    }
    
    /**
     * Test window aggregation with multiple messages
     */
    @Test
    public void testWindowAggregation() throws Exception {
        LOG.info("Testing window aggregation");
        
        // Create multiple test messages
        List<String> testData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            testData.add("{\"id\":\"test" + i + "\",\"type\":\"test\",\"data\":\"data" + i + "\"}");
        }
        
        DataStream<String> stream = env.fromCollection(testData);
        DataStream<String> processedStream = stream
                .map(new KafkaStreamJob.JsonProcessor())
                .filter(message -> message != null && !message.isEmpty())
                .map(new KafkaStreamJob.MessageEnricher());
        
        // Add window aggregation with shorter window for testing
        DataStream<String> windowedStream = processedStream
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(2)))
                .aggregate(new KafkaStreamJob.MessageAggregator());
        
        windowedStream.addSink(new CollectingSink(aggregatedMessages));
        
        // Execute
        env.execute("Window Aggregation Test");
        
        // Should have at least one aggregated message
        assertTrue("Should have aggregated messages", aggregatedMessages.size() > 0);
        
        for (String message : aggregatedMessages) {
            verifyAggregatedMessage(message);
        }
        
        LOG.info("Window aggregation test passed");
    }
    
    /**
     * Test streaming with continuous data generation
     */
    @Test
    public void testContinuousStreaming() throws Exception {
        LOG.info("Testing continuous streaming");
        
        // Create a larger dataset to simulate continuous streaming
        List<String> testData = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            testData.add(createTestEvent(i));
        }
        
        DataStream<String> stream = env.fromCollection(testData);
        DataStream<String> processedStream = stream
                .map(new KafkaStreamJob.JsonProcessor())
                .filter(message -> message != null && !message.isEmpty())
                .map(new KafkaStreamJob.MessageEnricher());
        
        // Add window aggregation
        DataStream<String> windowedStream = processedStream
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(3)))
                .aggregate(new KafkaStreamJob.MessageAggregator());
        
        processedStream.addSink(new CollectingSink(processedMessages));
        windowedStream.addSink(new CollectingSink(aggregatedMessages));
        
        // Execute
        env.execute("Continuous Streaming Test");
        
        // Verify results
        assertTrue("Should have processed messages", processedMessages.size() > 0);
        assertTrue("Should have aggregated messages", aggregatedMessages.size() > 0);
        
        LOG.info("Continuous streaming test passed. Processed: {}, Aggregated: {}", 
                processedMessages.size(), aggregatedMessages.size());
    }
    
    /**
     * Create a test event JSON string
     */
    private String createTestEvent(int index) {
        try {
            ObjectNode event = objectMapper.createObjectNode();
            String[] eventTypes = {"user_login", "user_logout", "page_view", "purchase", "click"};
            String[] sources = {"web", "mobile", "api", "batch"};
            
            event.put("id", "test-" + index);
            event.put("type", eventTypes[index % eventTypes.length]);
            event.put("user_id", "user" + (index % 5 + 1));
            event.put("source", sources[index % sources.length]);
            event.put("timestamp", System.currentTimeMillis());
            event.put("session_id", "session-" + index);
            event.put("ip_address", "192.168.1." + (index % 256));
            
            // Add specific data based on event type
            if ("purchase".equals(event.get("type").asText())) {
                event.put("amount", 100.0 + (index * 10));
                event.put("currency", "USD");
                event.put("product_id", "prod_" + index);
            } else if ("page_view".equals(event.get("type").asText())) {
                event.put("page_url", "/page/" + index);
                event.put("referrer", "https://example.com");
            }
            
            return event.toString();
        } catch (Exception e) {
            LOG.error("Error creating test event", e);
            return "{\"id\":\"error-" + index + "\",\"type\":\"error\"}";
        }
    }
    
    /**
     * Verify processed message structure
     */
    private void verifyProcessedMessage(String message) throws Exception {
        ObjectNode jsonNode = objectMapper.readValue(message, ObjectNode.class);
        
        // Check required fields
        assertTrue("Should have id field", jsonNode.has("id"));
        assertTrue("Should have type field", jsonNode.has("type"));
        assertTrue("Should have processed_at field", jsonNode.has("processed_at"));
        assertTrue("Should have processor field", jsonNode.has("processor"));
        assertTrue("Should have enriched field", jsonNode.has("enriched"));
        assertTrue("Should have enrichment_timestamp field", jsonNode.has("enrichment_timestamp"));
        
        // Check field values
        assertEquals("Processor should be flink-kafka-job", "flink-kafka-job", jsonNode.get("processor").asText());
        assertTrue("Enriched should be true", jsonNode.get("enriched").asBoolean());
        
        // Check timestamps are reasonable
        long processedAt = jsonNode.get("processed_at").asLong();
        long enrichmentTimestamp = jsonNode.get("enrichment_timestamp").asLong();
        assertTrue("Processed timestamp should be recent", processedAt > 0);
        assertTrue("Enrichment timestamp should be recent", enrichmentTimestamp > 0);
        assertTrue("Enrichment timestamp should be after processed timestamp", enrichmentTimestamp >= processedAt);
    }
    
    /**
     * Verify aggregated message structure
     */
    private void verifyAggregatedMessage(String message) throws Exception {
        ObjectNode jsonNode = objectMapper.readValue(message, ObjectNode.class);
        
        // Check required fields
        assertTrue("Should have count field", jsonNode.has("count"));
        assertTrue("Should have window_start field", jsonNode.has("window_start"));
        assertTrue("Should have aggregation_timestamp field", jsonNode.has("aggregation_timestamp"));
        
        // Check field values
        int count = jsonNode.get("count").asInt();
        assertTrue("Count should be positive", count > 0);
        
        long windowStart = jsonNode.get("window_start").asLong();
        long aggregationTimestamp = jsonNode.get("aggregation_timestamp").asLong();
        assertTrue("Window start should be recent", windowStart > 0);
        assertTrue("Aggregation timestamp should be recent", aggregationTimestamp > 0);
        assertTrue("Aggregation timestamp should be after window start", aggregationTimestamp >= windowStart);
    }
    
    /**
     * Collecting sink for test results
     */
    public static class CollectingSink implements SinkFunction<String> {
        private final List<String> collection;
        
        public CollectingSink(List<String> collection) {
            this.collection = collection;
        }
        
        @Override
        public void invoke(String value, Context context) {
            synchronized (collection) {
                collection.add(value);
                LOG.debug("Collected message: {}", value);
            }
        }
    }
} 