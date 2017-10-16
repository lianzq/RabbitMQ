package cn.lzq.mq.common;

/**
 * @author lianzhiqiang
 * Created in 2017/9/29
 */
public enum Action {
    ACCEPT,  // 处理成功
    RETRY,   // 可以重试的错误
    REJECT,  // 无需重试的错误
}
