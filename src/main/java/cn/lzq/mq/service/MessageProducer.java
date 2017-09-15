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

    /**
     * 发送消息，不保证事务，不入数据库
     *
     * @param exchange
     * @param routing
     * @param message
     * @return
     */
    boolean sendBySimple(String exchange, String routing, String message);
}
