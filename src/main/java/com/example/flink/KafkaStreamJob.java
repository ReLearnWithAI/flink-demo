package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Main Flink Kafka Streaming Job
 * 
 * This job demonstrates:
 * 1. Reading JSON messages from Kafka input topic
 * 2. Processing and transforming the data
 * 3. Writing processed results to Kafka output topic
 * 4. Window-based aggregations
 */
public class KafkaStreamJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Kafka configuration
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String GROUP_ID = "flink-kafka-group";
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(10000); // Checkpoint every 10 seconds
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Configure Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        
        // Create the data stream
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        
        // Process the stream
        DataStream<String> processedStream = stream
                .map(new JsonProcessor())
                .name("JSON Processor")
                .filter(message -> message != null && !message.isEmpty())
                .name("Filter Valid Messages")
                .map(new MessageEnricher())
                .name("Message Enricher");
        
        // Add window-based processing
        DataStream<String> windowedStream = processedStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .aggregate(new MessageAggregator())
                .name("Window Aggregator");
        
        // Send processed data to output topic
        processedStream.sinkTo(sink).name("Kafka Sink");
        
        // Send aggregated data to a different topic (optional)
        KafkaSink<String> aggregatedSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("aggregated-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        
        windowedStream.sinkTo(aggregatedSink).name("Aggregated Kafka Sink");
        
        LOG.info("Starting Flink Kafka Stream Job...");
        LOG.info("Reading from topic: {}", INPUT_TOPIC);
        LOG.info("Writing to topics: {} and aggregated-topic", OUTPUT_TOPIC);
        
        // Execute the job
        env.execute("Flink Kafka Stream Processing Job");
    }
    
    /**
     * Processes JSON messages and extracts relevant information
     */
    public static class JsonProcessor implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            try {
                ObjectNode jsonNode = objectMapper.readValue(value, ObjectNode.class);
                
                // Add processing timestamp
                jsonNode.put("processed_at", System.currentTimeMillis());
                jsonNode.put("processor", "flink-kafka-job");
                
                // Log the processed message
                LOG.debug("Processed message: {}", jsonNode.toString());
                
                return jsonNode.toString();
            } catch (Exception e) {
                LOG.error("Error processing JSON message: {}", value, e);
                return null;
            }
        }
    }
    
    /**
     * Enriches messages with additional metadata
     */
    public static class MessageEnricher implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            try {
                ObjectNode jsonNode = objectMapper.readValue(value, ObjectNode.class);
                
                // Add enrichment data
                jsonNode.put("enriched", true);
                jsonNode.put("enrichment_timestamp", System.currentTimeMillis());
                
                return jsonNode.toString();
            } catch (Exception e) {
                LOG.error("Error enriching message: {}", value, e);
                return value; // Return original value if enrichment fails
            }
        }
    }
    
    /**
     * Aggregates messages in a window
     */
    public static class MessageAggregator implements org.apache.flink.api.common.functions.AggregateFunction<String, ObjectNode, String> {
        @Override
        public ObjectNode createAccumulator() {
            return objectMapper.createObjectNode();
        }
        
        @Override
        public ObjectNode add(String value, ObjectNode accumulator) {
            try {
                ObjectNode message = objectMapper.readValue(value, ObjectNode.class);
                
                // Count messages
                int count = accumulator.has("count") ? accumulator.get("count").asInt() : 0;
                accumulator.put("count", count + 1);
                
                // Track window info
                accumulator.put("window_start", System.currentTimeMillis());
                accumulator.put("last_message_timestamp", message.has("processed_at") ? 
                    message.get("processed_at").asLong() : System.currentTimeMillis());
                
                return accumulator;
            } catch (Exception e) {
                LOG.error("Error aggregating message: {}", value, e);
                return accumulator;
            }
        }
        
        @Override
        public String getResult(ObjectNode accumulator) {
            try {
                accumulator.put("aggregation_timestamp", System.currentTimeMillis());
                return accumulator.toString();
            } catch (Exception e) {
                LOG.error("Error getting aggregation result", e);
                return "{}";
            }
        }
        
        @Override
        public ObjectNode merge(ObjectNode a, ObjectNode b) {
            try {
                int countA = a.has("count") ? a.get("count").asInt() : 0;
                int countB = b.has("count") ? b.get("count").asInt() : 0;
                a.put("count", countA + countB);
                return a;
            } catch (Exception e) {
                LOG.error("Error merging accumulators", e);
                return a;
            }
        }
    }
} 