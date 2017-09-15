package cn.lzq.mq.service;


/**
 * Created by lianzhiqiang on 2017/7/5.
 */
public interface MessageProcess {
    boolean process(String message) throws Exception;
}
