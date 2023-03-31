package top.simba1949.controller;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author anthony
 * @date 2023/3/31
 */
@RestController
@RequestMapping("mq")
public class RocketMQController {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("sendMqNoTag")
    public String sendMqNoTag(){

        rocketMQTemplate.convertAndSend("example_topic", "message no tag");

        return "SUCCESS";
    }

    @GetMapping("sendMqWithTag")
    public String sendMqWithTag(){
        rocketMQTemplate.convertAndSend("example_topic:example_tag", "message with tag");

        return "SUCCESS";
    }
}
