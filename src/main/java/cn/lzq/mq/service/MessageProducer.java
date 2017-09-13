package cn.lzq.mq.service;


/**
 * Created by lianzhiqiang on 2017/7/5.
 */
public interface MessageProducer {

    /**
     * 发送消息
     *
     * @param exchange
     * @param routing
     * @param message
     * @return
     */
    boolean send(String exchange, String routing, String message);
}
