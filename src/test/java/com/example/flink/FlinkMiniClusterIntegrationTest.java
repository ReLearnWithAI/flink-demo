package com.example.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.MiniClusterPipelineExecutorServiceLoader;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FlinkMiniClusterIntegrationTest {

    // 🧩 Start local Flink cluster
    private static final MiniClusterWithClientResource flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());

    @BeforeAll
    void setUp() throws Exception {
        flinkCluster.before();  // start the cluster
    }

    @AfterAll
    void tearDown() {
        flinkCluster.after();  // stop the cluster
    }

    @Test
    void testWordCountJobOnMiniCluster() throws Exception {
        // Use the cluster's config and execution backend
        Configuration config = new Configuration();
        config.setString("pipeline.executor", MiniClusterPipelineExecutorServiceLoader.NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        List<String> input = List.of("flink mini test", "flink rocks flink");

        DataStream<String> source = env.fromCollection(input);

        DataStream<Tuple2<String, Integer>> wordCounts = source
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0)
            .sum(1);

        wordCounts.print();  // for visual debug

        // ✅ Submit job to the MiniCluster
        JobExecutionResult result = env.execute("MiniCluster WordCount");

        assertNotNull(result);  // basic verification
    }

    // 🔠 Simple flatMap tokenizer
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String token : line.toLowerCase().split("\\W+")) {
                if (!token.isEmpty()) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
