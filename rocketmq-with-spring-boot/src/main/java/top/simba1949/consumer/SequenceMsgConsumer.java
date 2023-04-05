package top.simba1949.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * @author anthony
 * @date 2023/4/5
 */
@Slf4j
@Service
@RocketMQMessageListener(topic = "SEQUENCE_EXAMPLE_TOPIC",
        consumerGroup = "SEQUENCE_CONSUMER_GROUP",
        selectorExpression = "*")
public class SequenceMsgConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.info("当前时间【{}】获取到的消息是【{}】", System.currentTimeMillis(), message);
    }
}
