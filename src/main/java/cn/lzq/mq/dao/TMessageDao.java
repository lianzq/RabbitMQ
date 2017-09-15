package cn.lzq.mq.dao;

import java.util.List;
import java.util.Map;

/**
 * Created by xuhaojie on 17/6/20.
 */
public interface TMessageDao {

    long nextValue();

    boolean insert(long id, String message, Long createdOn);

    boolean insert(long id, String messageId, String message, Long createdOn);

    long insert(String message, Long createdOn);

    long insert(String messageId, String message, Long createdOn);

    boolean delete(long id);

    boolean deleteByStatus(long id);

    long insert(String message, Long createdOn, String exchangeName, String routing, String queueName, String type);

    long insert(String messageId, String message, Long createdOn, String exchangeName, String routing, String queueName, String type);

    long insert(String message, Long createdOn, String exchangeName, String routing, String queueName, String type, String appType);

    List<Map<String, Object>> queryAllMessage();

    List<Map<String, Object>> queryAllMessageByStatus();
}
