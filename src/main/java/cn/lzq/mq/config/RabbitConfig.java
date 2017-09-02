package cn.lzq.mq.config;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

/**
 * @author lianzhiqiang
 * Created in 下午4:31 2017/9/2
 */
public class RabbitConfig {

    /**
     * 配置链接信息
     * @param host
     * @param port
     * @param username
     * @param password
     * @return
     */
    @Bean
    public ConnectionFactory connectionFactory(
            @Value("${spring.rabbitmq.host}") String host,
            @Value("${spring.rabbitmq.port}") int port,
            @Value("${spring.rabbitmq.username}") String username,
            @Value("${spring.rabbitmq.password}") String password
    ){
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);

//        connectionFactory.setAddresses("host1:5672,host2:5672");
        connectionFactory.setPublisherConfirms(true); // enable confirm mode
        return connectionFactory;
    }
}
