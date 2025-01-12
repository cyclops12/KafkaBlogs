package com.rbcs.blog.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class KafkaBigMsg {

    private String bootStrapServer = "127.0.0.1:9092";
    private KafkaProducer<String, String> producer;

    private KafkaConsumer<String, String> consumer;

    public static void main(String[] args) throws Exception {
        new KafkaBigMsg().startTest();
    }

    void startTest() throws Exception {
        initProducer();
        String topicName = "bigMsgTopic";
        String payload = createBigStringMsg(2);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, payload);

        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        initConsumer();
        consumer.subscribe(Collections.singletonList(topicName));

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println(consumerRecord.key() + " :: " + consumerRecord.value());
        }

    }

    public String createBigStringMsg(int mbSize) {
        char[] data = new char[mbSize * 1024 * 1024];
        Arrays.fill(data, 'a');
        return new String(data);
    }

    public void initProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Produce side properties to increase request size
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "3024822");
        producer = new KafkaProducer<>(properties);
    }

    public void initConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigMsgConsumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");

        //consumer properties for big msg
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "3024822");
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "3024822");

        consumer = new KafkaConsumer<>(properties);
    }
}
