package top.simba1949;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author anthony
 * @date 2023/4/6
 */
public class TransactionMsgApplication {
    public static void main(String[] args) throws Exception {
        TransactionListenerImpl transactionListener = new TransactionListenerImpl();

        TransactionMQProducer producer = new TransactionMQProducer();
        producer.setNamesrvAddr("192.168.8.99:9876"); // 指定NameServer地址
        producer.setProducerGroup("TRANSACTION_PRODUCER_GROUP");
        producer.setTransactionListener(transactionListener); // 用于事务的监听

        // 定义一个线程池
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(@Nonnull Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                });
        producer.setExecutorService(executorService);
        producer.start(); // 启动Producer实例

        Message message = new Message();
        message.setTopic("TRANSACTION_TOPIC");
        message.setTags("TRANSACTION_TAG");
        message.setKeys("TRANSACTION_MESSAGE_KEY");
        message.setBody("事务消息内容".getBytes(StandardCharsets.UTF_8));

        SendResult sendResult = producer.sendMessageInTransaction(message, null);

        Thread.sleep(100000);
        producer.shutdown();
    }

    /**
     * 实现事务的监听接口
     */
    public static class TransactionListenerImpl implements TransactionListener {
        private AtomicInteger transactionIndex = new AtomicInteger(0);
        private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

        /**
         * 当发送半消息成功时，我们使用 executeLocalTransaction 方法来执行本地事务
         *
         * @param msg Half(prepare) message
         * @param arg Custom business parameter
         * @return
         */
        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            System.out.println("executeLocalTransaction" + msg.toString());

            int value = transactionIndex.getAndIncrement();
            int status = value % 3;
            localTrans.put(msg.getTransactionId(), status);
            return LocalTransactionState.UNKNOW; // 这里事务状态设置为 UNKNOW，用于 Broker 检查本地事务
        }

        /**
         * 用于回查本地事务状态，并回应消息队列的回查请求
         *
         * @param msg Check message
         * @return
         */
        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            System.out.println("checkLocalTransaction" + msg.toString());

            Integer status = localTrans.get(msg.getTransactionId());
            if (null != status) {
                switch (status) {
                    case 0:
                        return LocalTransactionState.UNKNOW;
                    case 1:
                        return LocalTransactionState.COMMIT_MESSAGE;
                    case 2:
                        return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }
            return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}
