package top.simba1949.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Service;

/**
 * selectorExpression
 *  - 默认 * ，表示不过滤 tag，消费此 Topic 下所有消息
 *  - 配置 tag ：单个直接 tag 名字，多个使用 || 分割
 * @author anthony
 * @date 2023/3/31
 */
@Slf4j
@Service
@RocketMQMessageListener(topic = "example_topic",
        consumerGroup = "example_consumer_group",
        selectorExpression = "*")
public class RocketMQConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.info("the message is {}", message);
    }
}