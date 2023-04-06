package top.simba1949.listener;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import top.simba1949.domain.Order;

/**
 * @author anthony
 * @date 2023/4/6
 */
@Slf4j
@Component
@RocketMQTransactionListener
public class TransactionListener implements RocketMQLocalTransactionListener {
    /**
     * 执行本地事务逻辑
     * @param message
     * @param arg
     * @return
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object arg) {
        log.info("执行本地事务代码，入参message={}, arg={}", message, arg);
        byte[] payload = (byte[]) message.getPayload();
        String messageContent = new String(payload); // 消息内容
        Order order = JSONObject.parseObject(messageContent, Order.class);

        if (null == order){
            return RocketMQLocalTransactionState.UNKNOWN;
        }
        // 通过订单信息，将订单提交到数据库，如果提交失败，则回滚整个事务，如果提交成功，则提交事务消息

        return RocketMQLocalTransactionState.COMMIT;
    }

    /**
     * 消息回查状态
     * 如果执行本地事务逻辑，事务是未知状态，会进行消息回查，确认最终消息状态；
     * 存在消息回查最大次数，达到最大次数后，Broker将丢弃次消息
     * @param message
     * @return
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        log.info("回查本地事务，入参message={}", message);
        byte[] payload = (byte[]) message.getPayload();
        String messageContent = new String(payload); // 消息内容
        Order order = JSONObject.parseObject(messageContent, Order.class);

        // RocketMQ 回查本地事务状态
        if (null == order){
            return RocketMQLocalTransactionState.UNKNOWN;
        }
        // 通过订单信息，查询数据库信息，如果查询到说明本地事务已经提交，则提交事务消息；如果查询不到说明本地事务已经回滚，则回滚事务消息

        return RocketMQLocalTransactionState.COMMIT;
    }
}
