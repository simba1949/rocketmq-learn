package top.simba1949.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author anthony
 * @date 2023/4/8
 */
@Slf4j
@RestController
@RequestMapping("filter")
public class SendTagFilterController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("tag")
    public String send4Tag(){
        Message<String> messageA = MessageBuilder.withPayload("TagA消息内容").build();
        Message<String> messageB = MessageBuilder.withPayload("TagB消息内容").build();
        Message<String> messageC = MessageBuilder.withPayload("TagC消息内容").build();

        SendResult sendResultA = rocketMQTemplate.syncSend("TAG_FILTER_TOPIC:TagA", messageA);
        log.info("TagA发送MQ结果是：{}", sendResultA);
        SendResult sendResultB = rocketMQTemplate.syncSend("TAG_FILTER_TOPIC:TagB", messageB);
        log.info("TagB发送MQ结果是：{}", sendResultB);
        SendResult sendResultC = rocketMQTemplate.syncSend("TAG_FILTER_TOPIC:TagC", messageC);
        log.info("TagC发送MQ结果是：{}", sendResultC);

        return "SUCCESS";
    }

    @GetMapping("sql")
    public String send4Sql(){
        Message<String> messageA = MessageBuilder.withPayload("TagA消息内容").setHeader("OrderType", "1").build();
        Message<String> messageB = MessageBuilder.withPayload("TagB消息内容").setHeader("OrderType", "2").build();
        Message<String> messageC = MessageBuilder.withPayload("TagC消息内容").setHeader("OrderType", "3").build();

        SendResult sendResultA = rocketMQTemplate.syncSend("TAG_FILTER_TOPIC:TagA", messageA);
        log.info("TagA发送MQ结果是：{}", sendResultA);
        SendResult sendResultB = rocketMQTemplate.syncSend("TAG_FILTER_TOPIC:TagB", messageB);
        log.info("TagB发送MQ结果是：{}", sendResultB);
        SendResult sendResultC = rocketMQTemplate.syncSend("TAG_FILTER_TOPIC:TagC", messageC);
        log.info("TagC发送MQ结果是：{}", sendResultC);

        return "SUCCESS";
    }
}
