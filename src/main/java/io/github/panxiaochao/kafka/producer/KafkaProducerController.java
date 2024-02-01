package io.github.panxiaochao.kafka.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code KafkaProducer}
 * <p> description: 生产者
 *
 * @author Lypxc
 * @since 2022-12-19
 */
@RestController
@RequestMapping("/kafka")
@RequiredArgsConstructor
public class KafkaProducerController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/send")
    public String send() {
        kafkaTemplate.send("KAFKA_TEST_TOPIC", "测试kafka消息" + System.currentTimeMillis());
        return "Success";
    }
}
