package com.gzl0ng.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author 郭正龙
 * @date 2022-04-08
 */
public class MyKafkaUtil {
    private static String brokers = "flink1:9092,flink2:9092,flink3:9092";
    private static String default_topic = "DWD_Default_topic";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>("flink1:9092,flink2:9092,flink3:9092", topic, new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<T>(default_topic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.NONE);//如果指定EXACTLY_ONCE，只要没有开启ck会自动重置为NONE
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static String getKafkaDDL(String topic, String groupId) {
        String ddl = "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                "	'properties.bootstrap.servers' = '" + brokers + "', " + " 'properties.group.id' = '" + groupId + "', " +
                "	'format' = 'json', " +
                "	'scan.startup.mode' = 'latest-offset'	";
        return ddl;
    }

}
