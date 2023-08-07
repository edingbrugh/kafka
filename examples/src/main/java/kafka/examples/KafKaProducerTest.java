package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * @Description: 生产者测试类
 * @Author NanKong
 * @Date 2022/9/1 20:13
 */
public class KafKaProducerTest {

    private static final String  brokerList = "10.51.134.132:9092";

    private static final String TOP_TEST = "test872800";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);

        //2.创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //3.封装消息队列
        for (Integer i = 0; i < 10; i++) {
            String key = "key" + i;
            String value = "value" + new Random().nextInt(3456789);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOP_TEST, key, value);
            producer.send(record);
        }
        producer.close();
    }
}
