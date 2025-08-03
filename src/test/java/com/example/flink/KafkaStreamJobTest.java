package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for KafkaStreamJob
 */
public class KafkaStreamJobTest extends AbstractTestBase {
    
    private StreamExecutionEnvironment env;
    private ObjectMapper objectMapper;
    
    @Before
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Use single parallelism for testing
        objectMapper = new ObjectMapper();
    }
    
    @Test
    public void testJsonProcessor() throws Exception {
        // Create test data
        String testJson = "{\"id\":\"test123\",\"type\":\"test\",\"data\":\"test data\"}";
        
        // Create a test stream
        List<String> testData = new ArrayList<>();
        testData.add(testJson);
        
        // Create a test sink to collect results
        CollectSink.values.clear();
        
        env.fromCollection(testData)
                .map(new KafkaStreamJob.JsonProcessor())
                .addSink(new CollectSink());
        
        env.execute("Test JsonProcessor");
        
        // Verify results
        assertEquals(1, CollectSink.values.size());
        String result = CollectSink.values.get(0);
        
        ObjectNode resultNode = objectMapper.readValue(result, ObjectNode.class);
        assertEquals("test123", resultNode.get("id").asText());
        assertEquals("test", resultNode.get("type").asText());
        assertEquals("test data", resultNode.get("data").asText());
        assertTrue(resultNode.has("processed_at"));
        assertEquals("flink-kafka-job", resultNode.get("processor").asText());
    }
    
    @Test
    public void testMessageEnricher() throws Exception {
        // Create test data with processing timestamp
        String testJson = "{\"id\":\"test123\",\"processed_at\":1234567890,\"processor\":\"test\"}";
        
        // Create a test stream
        List<String> testData = new ArrayList<>();
        testData.add(testJson);
        
        // Create a test sink to collect results
        CollectSink.values.clear();
        
        env.fromCollection(testData)
                .map(new KafkaStreamJob.MessageEnricher())
                .addSink(new CollectSink());
        
        env.execute("Test MessageEnricher");
        
        // Verify results
        assertEquals(1, CollectSink.values.size());
        String result = CollectSink.values.get(0);
        
        ObjectNode resultNode = objectMapper.readValue(result, ObjectNode.class);
        assertEquals("test123", resultNode.get("id").asText());
        assertTrue(resultNode.get("enriched").asBoolean());
        assertTrue(resultNode.has("enrichment_timestamp"));
    }
    
    @Test
    public void testMessageAggregator() throws Exception {
        // Create test data
        List<String> testData = new ArrayList<>();
        testData.add("{\"id\":\"test1\",\"processed_at\":1234567890}");
        testData.add("{\"id\":\"test2\",\"processed_at\":1234567891}");
        testData.add("{\"id\":\"test3\",\"processed_at\":1234567892}");
        
        // Create a test sink to collect results
        CollectSink.values.clear();
        
        env.fromCollection(testData)
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)))
                .aggregate(new KafkaStreamJob.MessageAggregator())
                .addSink(new CollectSink());
        
        env.execute("Test MessageAggregator");
        
        // Verify results
        assertEquals(1, CollectSink.values.size());
        String result = CollectSink.values.get(0);
        
        ObjectNode resultNode = objectMapper.readValue(result, ObjectNode.class);
        assertEquals(3, resultNode.get("count").asInt());
        assertTrue(resultNode.has("window_start"));
        assertTrue(resultNode.has("last_message_timestamp"));
        assertTrue(resultNode.has("aggregation_timestamp"));
    }
    
    @Test
    public void testInvalidJsonHandling() throws Exception {
        // Create test data with invalid JSON
        List<String> testData = new ArrayList<>();
        testData.add("invalid json");
        testData.add("{\"id\":\"test123\"}"); // Valid JSON
        
        // Create a test sink to collect results
        CollectSink.values.clear();
        
        env.fromCollection(testData)
                .map(new KafkaStreamJob.JsonProcessor())
                .filter(message -> message != null && !message.isEmpty())
                .addSink(new CollectSink());
        
        env.execute("Test Invalid JSON Handling");
        
        // Should only have one valid result
        assertEquals(1, CollectSink.values.size());
        String result = CollectSink.values.get(0);
        
        ObjectNode resultNode = objectMapper.readValue(result, ObjectNode.class);
        assertEquals("test123", resultNode.get("id").asText());
    }
    
    @Test
    public void testEmptyMessageHandling() throws Exception {
        // Create test data with empty messages
        List<String> testData = new ArrayList<>();
        testData.add("");
        testData.add(null);
        testData.add("{\"id\":\"test123\"}");
        
        // Create a test sink to collect results
        CollectSink.values.clear();
        
        env.fromCollection(testData)
                .map(new KafkaStreamJob.JsonProcessor())
                .filter(message -> message != null && !message.isEmpty())
                .addSink(new CollectSink());
        
        env.execute("Test Empty Message Handling");
        
        // Should only have one valid result
        assertEquals(1, CollectSink.values.size());
        String result = CollectSink.values.get(0);
        
        ObjectNode resultNode = objectMapper.readValue(result, ObjectNode.class);
        assertEquals("test123", resultNode.get("id").asText());
    }
    
    /**
     * Test sink for collecting results
     */
    public static class CollectSink implements SinkFunction<String> {
        public static final List<String> values = new ArrayList<>();
        
        @Override
        public void invoke(String value, Context context) {
            values.add(value);
        }
    }
} 