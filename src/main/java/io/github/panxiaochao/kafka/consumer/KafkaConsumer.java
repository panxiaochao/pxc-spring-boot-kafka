package io.github.panxiaochao.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * {@code KafkaConsumer}
 * <p> description: 消费者
 *
 * @author Lypxc
 * @since 2022-12-19
 */
@Component
public class KafkaConsumer {
    private static final Logger LOGGER = LogManager.getLogger(KafkaConsumer.class);

    /**
     * kafka的监听器
     */
    @KafkaListener(topics = {"KAFKA_TEST_TOPIC"}, groupId = "${spring.kafka.consumer.group-id}")
    public void topicListener(ConsumerRecord<String, String> record, Acknowledgment item) {
        LOGGER.info("Kafka监听：topic=" + record.topic() + ", partition=" + record.partition() + ", offset=" + record.offset() + ", message=" + record.value());
        // 手动提交
        item.acknowledge();
    }

    /**
     * kafka的监听器
     */
    @KafkaListener(topics = {"md-city-enroll"}, groupId = "${spring.kafka.consumer.group-id}")
    public void mdCityEnrollListener(ConsumerRecord<String, String> record, Acknowledgment item) {
        LOGGER.info("Kafka监听：topic=" + record.topic() + ", partition=" + record.partition() + ", offset=" + record.offset() + ", message=" + record.value());
        // 手动提交
        item.acknowledge();
    }
}
