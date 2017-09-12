package cn.lzq.mq.common;

/**
 * Created by lianzhiqiang on 2017/7/21.
 */
public enum MQExchangeTypeEnum {

    DIRECT("direct"), FANOUT("fanout"), TOPIC("topic"), HEADERS("headers");

    private final String type;

    MQExchangeTypeEnum(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
