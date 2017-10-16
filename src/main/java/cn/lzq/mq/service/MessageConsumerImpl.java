package cn.lzq.mq.service;

import cn.lzq.mq.common.Action;
import cn.lzq.mq.dao.MQRedisDao;
import com.rabbitmq.client.Channel;
import org.apache.commons.lang3.StringUtils;
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

    @Autowired
    protected MQRedisDao redisDao;

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
                Action action = Action.RETRY;
                try {
                    MessageConverter messageConverter = new Jackson2JsonMessageConverter();
                    Object messageBean = messageConverter.fromMessage(message);

                    // 去重操作，每个队列里面的消息处理成功后，放入redis。
                    // 每次消费消息的时候，先查询redis判断是否存在，存在则不处理，否则正常处理
                    if (messageBean != null) {
                        String queueName = message.getMessageProperties().getConsumerQueue();
                        String messageId = message.getMessageProperties().getMessageId();
                        if (StringUtils.isNotBlank(messageId) && StringUtils.isNotBlank(queueName)) {
                            if (redisDao.existKey(queueName + "_" + messageId)) {
                                channel.basicAck(message.getMessageProperties().getDeliveryTag(), true);
                            } else {
                                boolean isSuccess = messageProcess.process(messageBean.toString());
                                if (isSuccess) {
                                    // 成功处理消息，加入redis
                                    redisDao.setStringEx(queueName + "_" + messageId, 60, messageId);
                                    action = Action.ACCEPT;
                                } else {
                                    String key = "retry_" + queueName + "_" + messageId;
                                    if (redisDao.existKey(key)) {
                                        if (Integer.valueOf(redisDao.getString(key)) >= 3) {
                                            action = Action.REJECT;
                                        } else {
                                            redisDao.incr(key);
                                            action = Action.RETRY;
                                        }
                                    } else {
                                        redisDao.setString(key, "0");
                                        action = Action.RETRY;
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    action = Action.REJECT;
                } finally {
                    // 通过finally块来保证Ack/Nack会且只会执行一次
                    if (action == Action.ACCEPT) {
                        channel.basicAck(message.getMessageProperties().getDeliveryTag(), true);
                    } else if (action == Action.RETRY) {
                        channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                    } else {
                        channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
                    }
                }
            }
        };
    }
}
