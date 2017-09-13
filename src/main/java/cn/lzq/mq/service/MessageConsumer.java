package cn.lzq.mq.service;

/**
 * Created by lianzhiqiang on 2017/7/5.
 */
public interface MessageConsumer {
    /**
     * 消费者建立
     *
     * @param exchange
     * @param routing
     * @param queue
     * @param messageProcess
     */
    void consume(String exchange, String routing, String queue, MessageProcess messageProcess);

    /**
     * 消费者建立，指定数量
     *
     * @param exchange
     * @param routing
     * @param queue
     * @param messageProcess
     * @param consumeNum
     */
    void consume(String exchange, String routing, String queue, MessageProcess messageProcess, int consumeNum);
}
