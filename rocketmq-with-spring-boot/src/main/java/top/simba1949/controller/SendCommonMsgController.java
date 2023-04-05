package top.simba1949.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author anthony
 * @date 2023/4/5
 */
@Slf4j
@RestController
@RequestMapping("common")
public class SendCommonMsgController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    /**
     * 普通消息——发送同步消息
     *
     * @param msg
     * @return
     */
    @GetMapping("sendSyncMsg")
    public String sendSyncMsg(String msg) {
        // 第一个参数表示：主题和tag，可以不包含tag
        // 第二个参数表示：消息内容
        SendResult sendResult = rocketMQTemplate.syncSend("EXAMPLE_TOPIC:EXAMPLE_TAG", msg);
        if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
            log.info("消息发送成功");
        } else {
            log.info("消息发送失败");
        }

        return "sendSyncMsg";
    }

    /**
     * 普通消息——发送异步消息
     *
     * @param msg
     * @return
     */
    @GetMapping("sendAsyncMsg")
    public String sendAsyncMsg(String msg) {
        rocketMQTemplate.asyncSend(
                "EXAMPLE_TOPIC:EXAMPLE_TAG", // 消息的主题和tag
                msg, // 消息内容
                new SendCallback() { // 发送异步消息后，异步响应
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                            log.info("消息发送成功");
                        } else {
                            log.info("消息发送失败");
                        }
                    }
                    @Override
                    public void onException(Throwable e) {
                        // 发送消息异常时，处理的业务逻辑
                    }
                });
        return "sendAsyncMsg";
    }

    /**
     * 普通消息——发送单向消息
     * 单向发送消息是指，Producer仅负责发送消息，不等待、不处理MQ的ACK。该发送方式时MQ也不返回ACK。
     * @param msg
     * @return
     */
    @GetMapping("sendOneWayMsg")
    public String sendOneWayMsg(String msg) {
        // 第一个参数表示：主题和tag，可以不包含tag
        // 第二个参数表示：消息内容
        rocketMQTemplate.sendOneWay("EXAMPLE_TOPIC:EXAMPLE_TAG", msg);
        return "sendOneWayMsg";
    }
}
