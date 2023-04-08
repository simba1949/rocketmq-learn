package top.simba1949.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author anthony
 * @date 2023/4/6
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "TRANSACTION_TOPIC",
        consumerGroup = "TRANSACTION_CONSUMER_GROUP",
        selectorExpression = "*")
public class TransactionConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.info("收到事务消息：{}", message);
    }
}
