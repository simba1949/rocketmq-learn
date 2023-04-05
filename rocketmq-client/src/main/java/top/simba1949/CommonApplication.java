package top.simba1949;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author anthony
 * @date 2023/4/5
 */
public class CommonApplication {
    public static void main(String[] args) throws Exception {
        // 构建生产者
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("192.168.8.99:9876"); // 指定NameServer地址
        producer.setProducerGroup("PRODUCER_GROUP"); // 设置生产者组
        // 启动Producer实例
        producer.start();
        // 构建消息
        String msg = "sendOneWayMsg";
        Message message = new Message("EXAMPLE_TOPIC", "EXAMPLE_TAG", msg.getBytes(StandardCharsets.UTF_8));

        sendSyncMsg(producer, message); // 发送同步消息
        sendAsyncMsg(producer, message); // 发送异步消息
        sendOneWayMsg(producer, message); // 发送单向消息

        // 关闭生产者
        producer.shutdown();
    }

    /**
     * 发送同步消息
     * @param producer
     * @param message
     */
    public static void sendSyncMsg(MQProducer producer,Message message) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        // 发送消息
        SendResult sendResult = producer.send(message);
        // 判断响应结果
        if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
            System.out.println("消息发送成功");
        } else {
            System.out.println("消息发送失败");
        }
    }

    /**
     * 发送异步消息
     * @param producer
     * @param message
     */
    public static void sendAsyncMsg(MQProducer producer,Message message) throws RemotingException, InterruptedException, MQClientException {
        // 使用 CountDownLatch 等待响应结果
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                // 判断响应结果
                if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                    System.out.println("消息发送成功");
                } else {
                    System.out.println("消息发送失败");
                }
                countDownLatch.countDown();
            }
            @Override
            public void onException(Throwable e) {
                // 消费发送出现异常，处理逻辑
                countDownLatch.countDown();
            }
        });

        countDownLatch.await(10, TimeUnit.SECONDS);
    }

    /**
     * 发送单向消息
     * @param producer
     * @param message
     */
    public static void sendOneWayMsg(MQProducer producer,Message message) throws RemotingException, InterruptedException, MQClientException {
        // 发送单向消息，没有任何返回结果
        producer.sendOneway(message);
    }
}
