package cn.lzq.mq.service;

import cn.lzq.mq.common.MQExchangeTypeEnum;
import cn.lzq.mq.dao.TMessageDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by lianzhiqiang on 2017/7/20.
 */
@Service
public class MessageProducerImpl implements MessageProducer {

    @Autowired
    private TMessageDao tMessageDao;

    @Transactional
    @Override
    public boolean send(String exchangeName, String routing, String message) {

        try {
            long time = System.currentTimeMillis();
            tMessageDao.insert(message, time, exchangeName, routing, null, MQExchangeTypeEnum.TOPIC.getType());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

}
