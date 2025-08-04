package com.example.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SimpleFlinkMiniClusterTest {

    private static MiniCluster miniCluster;

    @BeforeAll
    static void startMiniCluster() throws Exception {
        MiniClusterConfiguration config = new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(2)
                .build();

        miniCluster = new MiniCluster(config);
        miniCluster.start();
    }

    @AfterAll
    static void stopMiniCluster() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test
    void testWordCountPipeline() throws Exception {
        Configuration config = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, config);

        List<String> input = List.of("hello world", "hello flink");

        DataStream<String> source = env.fromCollection(input);

        DataStream<Tuple2<String, Integer>> wordCounts = source
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        wordCounts.print(); // just for debug visibility

        JobExecutionResult result = env.execute("Local WordCount Test");

        // Basic assertion (just check job success here)
        assertNotNull(result);
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String token : value.toLowerCase().split("\\W+")) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
