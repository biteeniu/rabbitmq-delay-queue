package io.biteenew.rabbitmq.handler;

import com.rabbitmq.client.*;
import io.biteenew.rabbitmq.properties.RabbitmqClientOptions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 消息生产者<br />
 * 使用方式2：为队列设置TTL
 * @author luzhanghong
 * @date 2018-07-03 14:10
 */
public class Producer2 {

    private final static Logger LOGGER = LoggerFactory.getLogger(Producer2.class);
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;
    private final static String EXCHANGE = "out.exchange2";
    private final static String ROUTING_KEY = "out.routing.key2";
    private final static String DXL_EXCHANGE = "dlx.exchange2";
    private final static String DLX_ROUTING_KEY = "dlx.routing.key2";
    private final static String QUEUE = "buffer-queue2";

    public Producer2(RabbitmqClientOptions options) {
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
            // 创建交换机"out.exchange"：生产者将消息通过"out.exchange"发送到"buffer-queue"。这里设置交换机类型为"direct"，当然也可以使用其他类型
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
            // 创建死信交换机"dlx.exchange"："buffer-queue"中产生死信后，会通过此交换机发送出去。这里设置交换机类型为"direct"，当然也可以使用其他类型
            channel.exchangeDeclare(DXL_EXCHANGE, BuiltinExchangeType.DIRECT, true);
            // 创建缓冲队列"buffer-queue"，并为"buffer-queue"设置死信交换机参数：生产者发布的消息会先到达此"buffer-queue"，消息在"buffer-queue"中变成死信后，会通过死信交换机和死信路由键发送出去
            Map<String, Object> arguments = new HashMap<>();
            arguments.put("x-message-ttl", 5000);                        // 指定队列中消息最大存活时间（"x-message-ttl）
            arguments.put("x-dead-letter-exchange", DXL_EXCHANGE);       // 指定死信交换机参数（x-dead-letter-exchange）
            arguments.put("x-dead-letter-routing-key", DLX_ROUTING_KEY); // 指定死信路由键参数（x-dead-letter-routing-key）
            channel.queueDeclare(QUEUE, true, false, false, arguments);
            // 将缓冲队列"buffer-queue"绑定到交换机"out.exchange"，路由键为"out.routing.key"
            channel.queueBind(QUEUE, EXCHANGE, ROUTING_KEY);
            LOGGER.info("Producer connected to broker[{}:{}].", options.getHost(), options.getPort());
        } catch (Exception e) {
            LOGGER.error("Producer connect or declare exchange(queue) with error[{}:{}]: {}", options.getHost(), options.getPort(), e.getLocalizedMessage());
        }
    }

    /**
     * 发布消息
     * @param bytes 消息实体
     */
    public void publish(byte[] bytes) {
        try {
            channel.basicPublish(EXCHANGE, ROUTING_KEY, null, bytes);
        } catch (IOException e) {
            LOGGER.error("Producer publish message with error: {}", e.getLocalizedMessage());
        }
    }

}
