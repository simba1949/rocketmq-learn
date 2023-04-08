package top.simba1949.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author anthony
 * @date 2023/4/8
 */
@Slf4j
@Component
@RocketMQMessageListener(
        messageModel = MessageModel.CLUSTERING, // 消费模式，CLUSTERING表示集群消费，BROADCASTING表示广播消费
        topic = "TAG_FILTER_TOPIC", // 订阅的主题名称
        consumerGroup = "TAG_FILTER_CONSUMER_GROUP", // 设置消费者组名称
        selectorType = SelectorType.SQL92, // 消息过滤模式
        selectorExpression = "OrderType = 1 and OrderType = 2" // 消息过滤表达式
)
public class SqlFilterConsumer implements RocketMQListener<String> {
    @Override
    public void onMessage(String message) {
        log.info("SQL消息过滤消费者收到的消息时：{}", message);
    }
}
