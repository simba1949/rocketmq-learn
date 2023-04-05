package top.simba1949;

import com.alibaba.fastjson2.JSONObject;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

/**
 * @author anthony
 * @date 2023/4/5
 */
public class DelayMsgApplication {
    public static void main(String[] args) throws Exception {
        // 构建生产者
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("192.168.8.99:9876"); // 指定NameServer地址
        producer.setProducerGroup("PRODUCER_GROUP"); // 设置生产者组
        // 启动Producer实例
        producer.start();
        // 构建消息
        String msg = "延迟消息内容";
        String msgKey = "MessageKey"; // message key
        Message message = new Message("DELAY_TOPIC", "DELAY__TAG", msgKey, msg.getBytes(StandardCharsets.UTF_8));
        // private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";（从1到18等级）
        // message.setDelayTimeLevel(4); // 设置延迟等级
        message.setDelayTimeSec(30L); // 设置延迟秒数

        SendResult sendResult = producer.send(message);
        System.out.println("当前时间" + System.currentTimeMillis() + "收到" + JSONObject.toJSONString(sendResult) + "响应");

        // 关闭生产者，会从 Broker 中移除
        producer.shutdown();
    }
}
