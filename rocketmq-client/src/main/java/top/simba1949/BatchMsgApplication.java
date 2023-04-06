package top.simba1949;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * @author anthony
 * @date 2023/4/6
 */
public class BatchMsgApplication {
    public static void main(String[] args) throws Exception {
        // 构建生产者
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("192.168.8.99:9876"); // 指定NameServer地址
        producer.setProducerGroup("BATCH_PRODUCER_GROUP"); // 设置生产者组
        // 启动Producer实例
        producer.start();
        // 构建消息
        Message message1 = new Message("BATCH_TOPIC", "BATCH_TAG", "消息1".getBytes(StandardCharsets.UTF_8));
        Message message2 = new Message("BATCH_TOPIC", "BATCH_TAG", "消息2".getBytes(StandardCharsets.UTF_8));
        Message message3 = new Message("BATCH_TOPIC", "BATCH_TAG", "消息3".getBytes(StandardCharsets.UTF_8));
        Message message4 = new Message("BATCH_TOPIC", "BATCH_TAG", "消息4".getBytes(StandardCharsets.UTF_8));

        List<Message> messageList = Arrays.asList(message1, message2, message3, message4);
        // 批量同步发送消息
        producer.send(messageList);

        // 批量异步发送消息
        producer.send(messageList, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println(sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                System.out.println("批量异步发送消息异常处理逻辑");
            }
        });

        // 关闭生产者，会从 Broker 中移除
        producer.shutdown();
    }
}
