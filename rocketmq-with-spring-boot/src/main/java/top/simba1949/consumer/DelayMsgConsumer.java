package top.simba1949.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author anthony
 * @date 2023/4/5
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "DELAY_TOPIC",
        consumerGroup = "DELAY_CONSUMER_GROUP",
        selectorExpression = "*")
public class DelayMsgConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.info("时间：{}获取到延迟消息：{}", System.currentTimeMillis(), message);
    }
}
