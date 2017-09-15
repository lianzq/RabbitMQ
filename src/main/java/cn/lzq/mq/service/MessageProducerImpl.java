package cn.lzq.mq.service;

import cn.lzq.mq.common.MQExchangeTypeEnum;
import cn.lzq.mq.dao.TMessageDao;
import cn.lzq.mq.transaction.AfterCommitExecutor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Created by lianzhiqiang on 2017/7/20.
 */
@Service
public class MessageProducerImpl implements MessageProducer {

    @Autowired
    private TMessageDao tMessageDao;

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private AfterCommitExecutor afterCommitExecutor;

    @Transactional
    @Override
    public boolean send(String exchangeName, String routing, String message) {

        try {
            String messageId = UUID.randomUUID().toString();

            long time = System.currentTimeMillis();
            long id = tMessageDao.insert(messageId, message, time, exchangeName, routing, null, MQExchangeTypeEnum.TOPIC.getType());

            RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);

            RabbitTemplate rabbitTemplate = rabbitAdmin.getRabbitTemplate();
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


            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setMessageId(messageId);

            Message packMessage = rabbitTemplate.getMessageConverter().toMessage(message, messageProperties);

            afterCommitExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    //send message to mq
                    rabbitTemplate.convertAndSend(exchangeName, routing, packMessage, new CorrelationData(id + ""));
                    tMessageDao.deleteByStatus(id);
                }
            });
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean sendBySimple(String exchangeName, String routing, String message) {
        try {
            RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);

            RabbitTemplate rabbitTemplate = rabbitAdmin.getRabbitTemplate();
            rabbitTemplate.setMandatory(true);
            rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());

            String messageId = UUID.randomUUID().toString();
            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setMessageId(messageId);

            Message packMessage = rabbitTemplate.getMessageConverter().toMessage(message, messageProperties);

            rabbitTemplate.convertAndSend(exchangeName, routing, packMessage, new CorrelationData(StringUtils.EMPTY));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

}
