package top.simba1949;

import com.alibaba.fastjson2.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 顺序消息
 *
 * @author anthony
 * @date 2023/4/5
 */
public class SequenceMsgApplication {
    public static void main(String[] args) throws Exception {
        // 构建生产者
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("192.168.8.99:9876"); // 指定NameServer地址
        producer.setProducerGroup("SEQUENCE_PRODUCER_GROUP"); // 设置生产者组
        // 启动Producer实例
        producer.start();

        List<OrderStep> orderSteps = buildOrders();
        for (int i = 0; i < orderSteps.size(); i++) {
            OrderStep orderStep = orderSteps.get(i);

            Message message = new Message("SEQUENCE_EXAMPLE_TOPIC",
                    "SEQUENCE_EXAMPLE_TAG",
                    "MSG_KEY_" + i,
                    JSONObject.toJSONString(orderStep).getBytes(StandardCharsets.UTF_8));

            SendResult sendResult = producer.send(
                    message, // 构建的消息
                    new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            // 根据一个值（例如业务id）选择相同的队列
                            long orderId = (long) arg; // 根据订单id选择发送queue
                            long index = orderId % mqs.size();
                            return mqs.get((int) index);
                        }
                    },
                    orderStep.getOrderId()); // 订单id
        }


        // 关闭生产者，会从 Broker 中移除
        producer.shutdown();
    }

    /**
     * 订单顺序：创建——>付款——>推送——>完成
     * 时间顺序 ——————————————————————————————————————————————————————————————>
     * 订单1  |创建     |付款                                 |推送     |完成
     * 订单2  |    |创建         |付款                             |推送     |完成
     * 订单3  |           |创建     |付款     |推送     |完成
     *
     * @return
     */
    private static List<OrderStep> buildOrders() {
        long orderOne = 1L;
        long orderTwo = 2L;
        long orderThree = 3L;

        OrderStep orderOneCreate = OrderStep.builder().orderId(orderOne).desc("创建").build();
        OrderStep orderTwoCreate = OrderStep.builder().orderId(orderTwo).desc("创建").build();

        OrderStep orderOnePay = OrderStep.builder().orderId(orderOne).desc("付款").build();

        OrderStep orderThreeCreate = OrderStep.builder().orderId(orderThree).desc("创建").build();

        OrderStep orderTwoPay = OrderStep.builder().orderId(orderTwo).desc("付款").build();
        OrderStep orderThreePay = OrderStep.builder().orderId(orderThree).desc("付款").build();

        OrderStep orderThreePush = OrderStep.builder().orderId(orderThree).desc("推送").build();
        OrderStep orderThreeFinished = OrderStep.builder().orderId(orderThree).desc("完成").build();

        OrderStep orderOnePush = OrderStep.builder().orderId(orderOne).desc("推送").build();
        OrderStep orderTwoPush = OrderStep.builder().orderId(orderTwo).desc("推送").build();

        OrderStep orderOneFinished = OrderStep.builder().orderId(orderOne).desc("完成").build();
        OrderStep orderTwoFinished = OrderStep.builder().orderId(orderTwo).desc("完成").build();

        return Stream.of(orderOneCreate, orderTwoCreate, orderOnePay, orderThreeCreate,
                orderTwoPay, orderThreePay, orderThreePush, orderThreeFinished,
                orderOnePush, orderTwoPush, orderOneFinished, orderTwoFinished)
                .collect(Collectors.toList());
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class OrderStep {
        private Long orderId;
        private String desc;
    }
}
