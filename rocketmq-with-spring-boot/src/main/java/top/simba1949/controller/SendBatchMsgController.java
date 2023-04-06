package top.simba1949.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

/**
 * @author anthony
 * @date 2023/4/6
 */
@Slf4j
@RestController
@RequestMapping("batch")
public class SendBatchMsgController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping
    public String send() {
        Message<String> message1 = MessageBuilder.withPayload("批量消息1").build();
        Message<String> message2 = MessageBuilder.withPayload("批量消息2").build();
        Message<String> message3 = MessageBuilder.withPayload("批量消息3").build();
        Message<String> message4 = MessageBuilder.withPayload("批量消息4").build();

        List<Message<String>> messageList = Arrays.asList(message1, message2, message3, message4);

        // 同步批量发送
        SendResult sendResult4Sync = rocketMQTemplate.syncSend("BATCH_TOPIC:BATCH_TAG", messageList);
        log.info("同步批量发送消息返回结果{}", sendResult4Sync);

        // 异步批量发送
        rocketMQTemplate.asyncSend("BATCH_TOPIC:BATCH_TAG", messageList,
                new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        log.info("异步批量发送结果{}", sendResult);
                    }

                    @Override
                    public void onException(Throwable e) {
                        log.info("异步批量发送异常，异常信息{}", e.getMessage(), e);
                    }
                });

        return "SUCCESS";
    }
}
