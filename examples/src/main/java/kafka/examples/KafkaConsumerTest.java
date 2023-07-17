package kafka.examples;

import kafka.log.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Level;

/**
 * @Description: TODO 消费者测试类
 * @Author NanKong
 * @Date 2022/9/1 20:27
 */
public class KafkaConsumerTest {


    private static final String  brokerList = "localhost:9092";

    private static final String TOP_TEST = "test";

    public static void main(String[] args) {

        //1.创建Kafka链接参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", "group-topic_log");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2.创建Topic消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //3.订阅test01的消息队列
        kafkaConsumer.subscribe(Arrays.asList(TOP_TEST));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(100));
            Iterator<ConsumerRecord<String, String>> recordIterator = consumerRecords.iterator();
            if (recordIterator.hasNext()) {
                ConsumerRecord<String, String> record = recordIterator.next();
                String key = record.key();
                String value = record.value().toString();
                long offset = record.offset();
                int partition = record.partition();
                System.out.println("消费信息 key:" + key + ",value:" + value + ",partition:" + partition + ",offset:" + offset);
            }
        }
    }
}
