package com.ml;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream=env.fromElements("buy","sale","order","payment");

        // 配置 Kafka 连接属性
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "39.105.213.238:9092");

        KafkaSink<String> sink=KafkaSink.<String>builder()
                .setBootstrapServers("39.105.213.238:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("crm")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        stream.sinkTo(sink);



        env.execute();
    }
}
