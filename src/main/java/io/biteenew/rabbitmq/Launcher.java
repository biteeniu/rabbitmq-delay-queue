package io.biteenew.rabbitmq;

import io.biteenew.rabbitmq.handler.*;
import io.biteenew.rabbitmq.properties.RabbitmqClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 程序启动类
 * @author luzhanghong
 * @date 2018-07-02 10:07
 */
public class Launcher {

    private final static Logger LOGGER = LoggerFactory.getLogger(Launcher.class);

    /**
     * 程序启动方法
     * @param args args
     */
    public static void main(String[] args) {
        RabbitmqClientOptions options = new RabbitmqClientOptions()
                .setHost("10.200.0.206")
                .setPort(5672)
                .setUsername("rabbitmq")
                .setPassword("mZzMeaxhUZxqWm7F");

        // 消息延迟消费模式：方式1测试：为发布的每一条消息设置TTL
        // test1(options);
        // 消息延迟消费模式：方式2测试：为队列设置TTL
        // test2(options);
        // 消息延迟重试模式测试
        test3(options);
    }

    private static void test1(RabbitmqClientOptions options) {
        Producer1 producer1 = new Producer1(options);
        Consumer1 consumer1 = new Consumer1(options);
        // 测试1：生产5条消息，分别设置消息的TTL为：1秒，2秒，3秒，4秒，5秒
        for (int i = 1; i <= 5; i++) {
            String message = "" + i;
            LOGGER.info("Producer publish message: {}", message);
            producer1.publish(message.getBytes(), i*1000L);
        }
        // 测试2：生产5条消息，分别设置消息的TTL为：5秒，4秒，3秒，2秒，1秒
        for (int i = 5; i > 0; i--) {
            String message = "" + i;
            LOGGER.info("Producer publish message: {}", message);
            producer1.publish(message.getBytes(), i*1000L);
        }
    }

    private static void test2(RabbitmqClientOptions options) {
        Producer2 producer2 = new Producer2(options);
        Consumer2 consumer2 = new Consumer2(options);
        // 测试1：生产5条消息，分别设置消息的TTL为：1秒，2秒，3秒，4秒，5秒
        for (int i = 1; i <= 5; i++) {
            String message = "" + i;
            LOGGER.info("Producer publish message: {}", message);
            producer2.publish(message.getBytes());
        }
    }

    private static void test3(RabbitmqClientOptions options) {
        Producer3 producer3 = new Producer3(options);
        Consumer3 consumer3 = new Consumer3(options);
        for (int i = 1; i <= 5; i++) {
            String message = "" + i;
            LOGGER.info("Producer publish message: {}", message);
            producer3.publish(message.getBytes());
        }
    }

}
