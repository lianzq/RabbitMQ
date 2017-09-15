package cn.lzq.mq.service;

/**
 * Created by lianzhiqiang on 2017/7/21.
 */
public interface MQSendTaskService {

    /**
     * 扫描数据库表，发送信息
     */
    void scanTableAndSendMessage() throws InterruptedException;
}
