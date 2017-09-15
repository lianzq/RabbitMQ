package cn.lzq.mq.dao;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Created by xuhaojie on 17/6/20.
 */
@Repository
public class TMessageDaoImpl implements TMessageDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public long nextValue() {
        String sql = "select seq_mq_message.nextval from dual";
        long value = jdbcTemplate.queryForObject(sql, BigDecimal.class).longValue();
        return value;
    }

    @Override
    public long insert(String message, Long time) {
        long id = nextValue();
        boolean insert = insert(id, message, time);
        return id;
    }

    @Override
    public long insert(String messageId, String message, Long time) {
        long id = nextValue();
        boolean insert = insert(id, messageId, message, time);
        return id;
    }

    @Override
    public boolean insert(long id, String message, Long time) {
        String sql = "insert into mq_message(id,message,time) values(?,?,?)";
        int rows = jdbcTemplate.update(sql, id, message, time);
        return rows == 1;
    }

    @Override
    public boolean insert(long id, String messageId, String message, Long time) {
        String sql = "insert into mq_message(id, message_id, message,time) values(?,?,?,?)";
        int rows = jdbcTemplate.update(sql, id, messageId, message, time);
        return rows == 1;
    }

    @Override
    public boolean delete(long id) {
        String sql = "delete from mq_message where id = ? ";
        int rows = jdbcTemplate.update(sql, id);
        return rows == 1;
    }

    @Override
    public boolean deleteByStatus(long id) {
        String sql = "UPDATE mq_message SET status = 1 where id = ? ";
        int rows = jdbcTemplate.update(sql, id);
        return rows == 1;
    }

    @Override
    public long insert(String message, Long time, String exchangeName, String routing, String queueName, String type) {
        long id = nextValue();
        String sql = "insert into mq_message(id, message,time, EXCHANGE_NAME, ROUTING_KEY, QUEUE_NAME, EXCHANGE_TYPE) values(?,?,?,?,?,?,?)";
        int rows = jdbcTemplate.update(sql, id, message, time, exchangeName, routing, queueName, type);
        return rows;
    }

    @Override
    public long insert(String message, Long time, String exchangeName, String routing, String queueName, String type, String appType) {
        long id = nextValue();
        String sql = "insert into mq_message(id, message,time, EXCHANGE_NAME, ROUTING_KEY, QUEUE_NAME, EXCHANGE_TYPE, APP_TYPE) values(?,?,?,?,?,?,?,?)";
        int rows = jdbcTemplate.update(sql, id, message, time, exchangeName, routing, queueName, type, appType);
        return rows;
    }

    @Override
    public List<Map<String, Object>> queryAllMessage() {
        String sql = " select * from mq_message ";
        return jdbcTemplate.queryForList(sql);
    }

    @Override
    public List<Map<String, Object>> queryAllMessageByStatus() {
        String sql = " select * from mq_message where status = 0 ";
        return jdbcTemplate.queryForList(sql);
    }
}
