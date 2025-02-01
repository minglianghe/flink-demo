package com.ml;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("39.105.213.238:9092")
                .setTopics("user")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        kafka_source.print();

        env.execute();

    }
}
