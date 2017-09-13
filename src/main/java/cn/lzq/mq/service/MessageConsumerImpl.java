package cn.lzq.mq.service;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by lianzhiqiang on 2017/7/20.
 */
@Service
public class MessageConsumerImpl implements MessageConsumer {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Override
    public void consume(String exchangeName, String routing, String queueName, MessageProcess messageProcess) {

        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);

        TopicExchange topicExchange = new TopicExchange(exchangeName);
        rabbitAdmin.declareExchange(topicExchange);

        Queue queue = new Queue(queueName);
        rabbitAdmin.declareQueue(queue);

        Binding binding = BindingBuilder.bind(queue).to(topicExchange).with(routing);
        rabbitAdmin.declareBinding(binding);

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(queue);
        container.setExposeListenerChannel(true);
        container.setPrefetchCount(1);
        container.setConcurrentConsumers(Runtime.getRuntime().availableProcessors());//按照jvmCPU核数配置
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener(getMessageListener(messageProcess));

        container.start();
    }

    @Override
    public void consume(String exchangeName, String routing, String queueName, MessageProcess messageProcess, int consumeNum) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);

        TopicExchange topicExchange = new TopicExchange(exchangeName);
        rabbitAdmin.declareExchange(topicExchange);

        Queue queue = new Queue(queueName);
        rabbitAdmin.declareQueue(queue);

        Binding binding = BindingBuilder.bind(queue).to(topicExchange).with(routing);
        rabbitAdmin.declareBinding(binding);

        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueues(queue);
        container.setExposeListenerChannel(true);
        container.setPrefetchCount(1);
        container.setConcurrentConsumers(consumeNum);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener(getMessageListener(messageProcess));

        container.start();
    }


    private ChannelAwareMessageListener getMessageListener(final MessageProcess messageProcess) {
        return new ChannelAwareMessageListener() {
            @Override
            public void onMessage(Message message, Channel channel) throws Exception {
                MessageConverter messageConverter = new Jackson2JsonMessageConverter();
                Object messageBean = messageConverter.fromMessage(message);
                boolean isSuccess = messageProcess.process(messageBean.toString());
                if (isSuccess) {
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), true);
                } else {
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                }
            }
        };
    }
}
