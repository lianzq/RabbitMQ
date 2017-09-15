package cn.lzq.mq.service;


import cn.lzq.mq.dao.TMessageDao;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by lianzhiqiang on 2017/7/21.
 */
@Service
public class MQSendTaskServiceImpl implements MQSendTaskService {


    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private TMessageDao tMessageDao;

    @Override
    public void scanTableAndSendMessage() throws InterruptedException {
        List<Map<String, Object>> messageList = tMessageDao.queryAllMessageByStatus();

        if (CollectionUtils.isNotEmpty(messageList)) {
            for (Map<String, Object> map : messageList) {
                Long id = Long.parseLong(map.get("id").toString());
                String exchangeName = (String) map.get("EXCHANGE_NAME");
                String routingKey = (String) map.get("ROUTING_KEY");
                Object message = map.get("MESSAGE");

                send(id, exchangeName, routingKey, message);
                tMessageDao.deleteByStatus(id);
            }

        } else {
            Thread.sleep(1000);
        }
    }

    private void send(Long id, String exchangeName, String routing, Object message) {

        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);

        RetryTemplate retryTemplate = new RetryTemplate();
        /**
         * 指数退避策略，需设置参数sleeper、initialInterval、maxInterval和multiplier，
         * initialInterval指定初始休眠时间，默认100毫秒，
         * maxInterval指定最大休眠时间，默认30秒，
         * multiplier指定乘数，即下一次休眠时间为当前休眠时间*multiplier
         *
         */
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(500);
        backOffPolicy.setMultiplier(10.0);
        backOffPolicy.setMaxInterval(10000);

        retryTemplate.setBackOffPolicy(backOffPolicy);

        RabbitTemplate rabbitTemplate = rabbitAdmin.getRabbitTemplate();
        rabbitTemplate.setRetryTemplate(retryTemplate);
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());

        rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
            @Override
            public void confirm(CorrelationData correlationData, boolean ack, String cause) {
                if (ack) {
                    tMessageDao.delete(Long.valueOf(correlationData.getId()));
                }
            }
        });

        try {

            String messageId = UUID.randomUUID().toString();
            org.springframework.amqp.core.MessageProperties messageProperties = new org.springframework.amqp.core.MessageProperties();
            messageProperties.setMessageId(messageId);

            Message packMessage = rabbitTemplate.getMessageConverter().toMessage(message, messageProperties);

            rabbitTemplate.convertAndSend(exchangeName, routing, packMessage, new CorrelationData(id + ""));

        } catch (Exception e) {
        }
    }
}
