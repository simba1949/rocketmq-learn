package top.simba1949.controller;

import com.alibaba.fastjson2.JSONObject;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

/**
 * @author anthony
 * @date 2023/4/5
 */
@RestController
@RequestMapping("delay")
public class SendDelayMsgController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("send")
    public String send(){
        SendResult sendResult = rocketMQTemplate.syncSendDelayTimeSeconds(
                "DELAY_TOPIC:DELAY__TAG", // 主题和Tag
                "延迟消息".getBytes(StandardCharsets.UTF_8), // 消息内容
                30); // 延迟时间
        return JSONObject.toJSONString(sendResult);
    }
}
