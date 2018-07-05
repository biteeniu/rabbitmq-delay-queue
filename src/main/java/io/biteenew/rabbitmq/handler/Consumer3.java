package io.biteenew.rabbitmq.handler;

import com.rabbitmq.client.*;
import io.biteenew.rabbitmq.properties.RabbitmqClientOptions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * 消息消费者<br />
 * 消息延迟重试模型的消费者：使用方式2：为队列设置TTL
 * @author luzhanghong
 * @date 2018-07-03 15:21
 */
public class Consumer3 {

    private final static Logger LOGGER = LoggerFactory.getLogger(Consumer3.class);
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;
    private final static String EXCHANGE = "out.exchange3";
    private final static String ROUTING_KEY = "out.routing.key3";
    private final static String DXL_EXCHANGE = "dlx.exchange3";
    private final static String DLX_ROUTING_KEY = "dlx.routing.key3";
    private final static String QUEUE = "worker-queue3";
    private final static String FAILED_EXCHANGE = "failed.exchange";
    private final static String FAILED_ROUTING_KEY = "failed.routing.key";
    private final static Random random = new Random();

    public Consumer3(RabbitmqClientOptions options) {
        init(options);
        connectAndDeclare(options);
    }

    /**
     * 初始化连接参数
     * @param options RabbitMQ客户端参数
     */
    private void init(RabbitmqClientOptions options) {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(options.getHost());
        connectionFactory.setPort(options.getPort());
        if (StringUtils.isNoneBlank(options.getUsername(), options.getPassword())) {
            connectionFactory.setUsername(options.getUsername());
            connectionFactory.setPassword(options.getPassword());
        }
        // 设置连接超时时间：20秒
        connectionFactory.setConnectionTimeout(20000);
    }

    /**
     * 连接RabbitMQ服务器并创建相关的交换机和队列
     * @param options RabbitMQ客户端参数
     */
    private void connectAndDeclare(RabbitmqClientOptions options) {
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(DXL_EXCHANGE, BuiltinExchangeType.DIRECT, true);
            channel.queueDeclare(QUEUE, true, false, false, null);
            // 将"worker-queue"绑定到交换机"out.exchange"，路由键为"out.routing.key"
            channel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
            // 同时将"worker-queue"绑定到失败交换机"dlx.exchange"，路由键为"dlx.routing.key"
            channel.queueBind(QUEUE, DXL_EXCHANGE, DLX_ROUTING_KEY);
            channel.basicConsume(QUEUE, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    if (random.nextBoolean()) {
                        LOGGER.info("Consumer received message: {} -- handle ok.", new String(body));
                    } else {
                        LOGGER.info("Consumer received message: {} -- handle failed, will retry in 10 seconds.", new String(body));
                        // 将处理失败的消息丢到"buffer-queue"
                        failedPublish(body);
                    }
                }
            });
            LOGGER.info("Consumer connected to broker[{}:{}].", options.getHost(), options.getPort());
        } catch (Exception e) {
            LOGGER.error("Consumer connect or declare exchange(queue) with error[{}:{}]: {}", options.getHost(), options.getPort(), e.getLocalizedMessage());
        }
    }

    /**
     * 将处理失败的消息发布到buffer-queue中
     * @param bytes 消息实体
     */
    public void failedPublish(byte[] bytes) {
        try {
            channel.basicPublish(FAILED_EXCHANGE, FAILED_ROUTING_KEY, null, bytes);
        } catch (IOException e) {
            LOGGER.error("Consumer publish failed-handle message with error: {}", e.getLocalizedMessage());
        }
    }

}
