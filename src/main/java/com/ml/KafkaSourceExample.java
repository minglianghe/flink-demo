package com.ml;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSourceExample {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 KafkaSource
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("39.105.213.238:9092")
                .setTopics("test-topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 从 Kafka 读取数据
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 打印接收到的数据
        stream.print();


        KafkaSink<String> sink=KafkaSink.<String>builder()
                .setBootstrapServers("39.105.213.238:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("crm")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        stream.writeAsText("d:\\temp\\flink");

        //stream.sinkTo(sink);

        // 执行作业
        env.execute("Kafka Source Example");
    }
}
