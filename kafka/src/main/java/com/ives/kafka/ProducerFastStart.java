package com.ives.kafka;

import kafka.tools.ConsoleProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerFastStart {

    private static final String brokerList = "35.229.195.168:9092";
    private static final String topic = "ivesmsg";

    public static void main(String[] args) {
        Properties properties = new Properties();

        // 設置key序列化器
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 設置重試次數
        properties.put(ProducerConfig.RETRIES_CONFIG,10);

        // 設置值序列化器
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 設置集群地址
        properties.put("bootstrap.servers",brokerList);
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,"kafka-demo","i am java API");

        try {
            producer.send(record);
        }catch (Exception e){
            e.printStackTrace();
        }

        producer.close();
    }
}
