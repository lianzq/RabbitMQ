package cn.lzq.mq.dao;

import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhangwei
 * @date 2017/8/10
 */
public interface MQRedisDao {

    String getString(String key);

    List<String> mget(String... keys);

    String setString(String key, String value);

    String setStringEx(String key, int seconds, String value);

    String setStringExAtMillisTimestamp(String key, long timeMillis, String value);

    Long expireAtMillisTimestamp(String key, long timeMillis);

    long setnx(String key, String value, int seconds);

    Set<String> keys(String pattern);

    long delByPattern(String pattern);

    long del(String... keys);

    //void returnJedis(Jedis jedis);

    //void returnBrokenJedis(Jedis jedis);

    boolean existKey(String key);

    void release(Jedis jedis, boolean isBroken);

    void setObjex(String strKey, int seconds, Object obj);

    void setObj(String strKey, Object obj);

    Object getObj(String strKey, Class objClass);

    Object getObjex(String strKey, Class objClass);

    long delObj(String strKey, Class objClass);

    long delObjs(String[] strKeys, Class objClass);

    byte[] serialize(Object object);

    Object unserialize(byte[] bytes);

    Long lpush(String key, String... strings);

    Long rpush(String key, String... strings);

    List<String> lrange(String key, long start, long end);

    Long llen(String key);

    Long hset(String key, String field, String value);

    String hget(String key, String field);

    Set<String> hkeys(String key);

    List<String> hvals(String key);

    String hmset(String key, Map<String, String> hash);

    List<String> hmget(String key, String... fields);

    Map<String, String> hgetall(String key);

    Long hdel(String key, String... fields);

    Long zadd(String key, double score, String member);

    Long zadd(String key, Map<String, Double> scoreMembers);

    Set<String> zrangeByScore(String key, double min, double max);

    Set<String> zrangeByScore(String key, double min, double max,
                              int offset, int count);

    Set<String> zrange(String key, long start, long end);

    Long zrem(String key, String... members);

    Double zscore(String key, String member);

    Double zincrby(String key, double score, String member);

    Long expire(String key, int seconds);

    Long ttl(String key);

    void flashAll();

    Long incr(String key);
}
