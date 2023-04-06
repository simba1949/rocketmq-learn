package top.simba1949.controller;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import top.simba1949.domain.Order;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author anthony
 * @date 2023/4/6
 */
@Slf4j
@RestController
@RequestMapping("transaction")
public class SendTransactionMsgController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping
    public String send(){
        String orderCode = "ORDER-2023040601";

        List<String> goodList = Stream.of("苹果", "香蕉", "橘子").collect(Collectors.toList());
        Order order = Order.builder().orderCode(orderCode).goodList(goodList).build();

        Message<Order> message = MessageBuilder
                .withPayload(order)
                .setHeader(RocketMQHeaders.KEYS, orderCode) // 消息头，底层是 Map，可以设置多个
                .build();

        TransactionSendResult transactionSendResult = rocketMQTemplate.sendMessageInTransaction("TRANSACTION_TOPIC:TRANSACTION_TAG", message, null);

        return JSONObject.toJSONString(transactionSendResult);
    }
}
