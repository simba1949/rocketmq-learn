package top.simba1949.controller;

import com.alibaba.fastjson2.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author anthony
 * @date 2023/4/5
 */
@Slf4j
@RestController
@RequestMapping("sequence")
public class SendSequenceMsgController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("send")
    public String send(){
        List<OrderStep> orderSteps = buildOrders();

        for (OrderStep orderStep : orderSteps) {
            SendResult sendResult = rocketMQTemplate.syncSendOrderly(
                    "SEQUENCE_EXAMPLE_TOPIC:SEQUENCE_EXAMPLE_TAG", // 主题和Tag
                    JSONObject.toJSONString(orderStep), // 消息内容
                    String.valueOf(orderStep.getOrderId())); // 指定需要分配到同一个Queue的key
            log.info("the result is {}", JSONObject.toJSONString(sendResult));
        }

        return "SUCCESS";
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
