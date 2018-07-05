package io.biteenew.rabbitmq.handler;

import com.rabbitmq.client.*;
import io.biteenew.rabbitmq.properties.RabbitmqClientOptions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 消息消费者<br />
 * 使用方式1：为发布的每条消息单独设置TTL
 * @author luzhanghong
 * @date 2018-07-03 15:21
 */
public class Consumer2 {

    private final static Logger LOGGER = LoggerFactory.getLogger(Consumer2.class);
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private final static String DXL_EXCHANGE = "dlx.exchange2";
    private final static String DLX_ROUTING_KEY = "dlx.routing.key2";
    private final static String QUEUE = "worker-queue2";

    public Consumer2(RabbitmqClientOptions options) {
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
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(DXL_EXCHANGE, BuiltinExchangeType.DIRECT, true);
            channel.queueDeclare(QUEUE, true, false, false, null);
            channel.queueBind(QUEUE, DXL_EXCHANGE, DLX_ROUTING_KEY);
            channel.basicConsume(QUEUE, true, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    LOGGER.info("Consumer received message: {}", new String(body));
                }
            });
            LOGGER.info("Consumer connected to broker[{}:{}].", options.getHost(), options.getPort());
        } catch (Exception e) {
            LOGGER.error("Consumer connect or declare exchange(queue) with error[{}:{}]: {}", options.getHost(), options.getPort(), e.getLocalizedMessage());
        }
    }


}
