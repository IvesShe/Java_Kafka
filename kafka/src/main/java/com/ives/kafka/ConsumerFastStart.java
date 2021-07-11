package com.ives.kafka;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerFastStart {

    private static final String brokerList = "35.229.195.168:9092";
    private static final String topic = "ivesmsg";
    private static final String groupId = "group.demo";

    public static void main(String[] args) {

        Properties properties = new Properties();

        // 設置key序列化器
//        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 設置重試次數
        //properties.put(ProducerConfig.RETRIES_CONFIG,10);

        // 設置值序列化器
//        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        // 設置集群地址
//        properties.put("bootstrap.servers",brokerList);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);

//        properties.put("group.id",groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records){
                System.out.println(record.value());
            }
        }
    }
}
