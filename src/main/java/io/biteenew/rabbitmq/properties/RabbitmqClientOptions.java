package io.biteenew.rabbitmq.properties;

/**
 * RabbitMQ客户端参数定义类
 * @author luzhanghong
 * @date 2018-07-02 13:25
 */
public class RabbitmqClientOptions {

    private String host;      // RabbitMQ服务器地址（域名或IP）
    private Integer port;     // RabbitMQ服务器端口
    private String username;  // RabbitMQ服务器登录用户名
    private String password;  // RabbitMQ服务器登录密码

    public String getHost() {
        return host;
    }

    public RabbitmqClientOptions setHost(String host) {
        this.host = host;
        return this;
    }

    public Integer getPort() {
        return port;
    }

    public RabbitmqClientOptions setPort(Integer port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public RabbitmqClientOptions setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public RabbitmqClientOptions setPassword(String password) {
        this.password = password;
        return this;
    }

}
