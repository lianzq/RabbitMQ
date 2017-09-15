package cn.lzq.mq.controller;

import cn.lzq.mq.common.MQExchange;
import cn.lzq.mq.common.MQQueue;
import cn.lzq.mq.common.MQRoutingKey;
import cn.lzq.mq.service.MessageConsumer;
import cn.lzq.mq.service.MessageProcess;
import cn.lzq.mq.service.MessageProducer;
import cn.lzq.mq.util.RandomStringUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by lianzhiqiang on 2017/7/20.
 */
@RestController
@RequestMapping("/example")
public class MQController {

    @Autowired
    private MessageConsumer messageConsumer;

    @Autowired
    private MessageProducer messageProducer;

    @Autowired
    @Qualifier("jsonMessageProcessImpl")
    private MessageProcess messageProcess;

    @RequestMapping("/consume")
    public String consume(HttpServletRequest request) {
        messageConsumer.consume(MQExchange.LZQ_CRM, MQRoutingKey.LZQ_CRM_ORDER_CREATE, MQQueue.LZQ_CRM_ORDER, messageProcess);
        return "{\"result\":\"0\", \"msg\":\"消费者启动！\"}";
    }

    @RequestMapping("/send")
    public String send(HttpServletRequest request) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", System.currentTimeMillis());
        jsonObject.put("orderNum", RandomStringUtil.getRandomString(12));
        messageProducer.send(MQExchange.LZQ_CRM, MQRoutingKey.LZQ_CRM_ORDER_CREATE, jsonObject.toString());
        return "{\"result\":\"0\", \"msg\":\"" + jsonObject + "\"}";
    }
}
