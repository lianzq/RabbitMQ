package cn.lzq.mq.dao;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhangwei
 * @date 2017/8/10
 */
@Repository
public class MQRedisDaoImpl implements MQRedisDao {
    private static final Logger LOG = Logger.getLogger(MQRedisDaoImpl.class);

    @Autowired
    @Qualifier("jedisPool")
    private JedisPool mqJedisPool;

    /**
     * 从jedis连接池中获取获取jedis对象
     *
     * @return
     */
    private Jedis getJedis() {
        Jedis jedis = null;
        try {
            jedis = mqJedisPool.getResource();
        } catch (JedisException e) {
            LOG.error(e.getLocalizedMessage(), e);
            returnBrokenJedis(jedis);
            throw e;
        }
        return jedis;
    }

    /**
     * 回收资源
     *
     * @param jedis
     */
    private void returnJedis(Jedis jedis) {
        if (jedis != null) {
            mqJedisPool.returnResource(jedis);
        }
    }

    /**
     * Jedis连接发生错误时，必须调用returnBrokenResource返还给pool，否则下次通过getResource得到的instance的缓冲区可能还存在数据，出现问题
     *
     * @param jedis
     */
    private void returnBrokenJedis(Jedis jedis) {
        if (jedis != null) {
            mqJedisPool.returnBrokenResource(jedis);
        }
    }

    /**
     * 根据key获取记录
     *
     * @param key
     * @return 值
     */
    @Override
    public String getString(String key) {
        String value = null;
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            if (jedis.exists(key)) {
                value = jedis.get(key);
                value = StringUtils.isNotBlank(value) && !"null".equalsIgnoreCase(value) ? value : null;
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return value;
    }

    /**
     * 批量获取指定的多个key的值，如果key不存在，则返回'nil'，此方法不会失败。
     * Get the values of all the specified keys. If one or more keys dont exist or is not of type
     * String, a 'nil' value is returned instead of the value of the specified key, but the operation
     * never fails.
     *
     * @param keys 指定的一个或多个key
     * @return
     */
    @Override
    public List<String> mget(final String... keys) {
        List<String> values = null;
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            values = jedis.mget(keys);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return values;
    }

    /**
     * 添加值
     *
     * @param key
     * @param value
     * @return String 操作状态
     */
    @Override
    public String setString(String key, String value) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            String str = jedis.set(key, value);
            return str;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 添加有过期时间的记录
     *
     * @param key
     * @param seconds 过期时间，以秒为单位
     * @param value
     * @return String 操作状态
     */
    @Override
    public String setStringEx(String key, int seconds, String value) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            String str = jedis.setex(key, seconds, value);
            return str;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 添加有过期时间的记录
     *
     * @param key        Redis数据的KEY
     * @param timeMillis Java时间戳（毫秒数），如 date.getTime(), System.currentTimeMillis()
     * @param value      key对应的值
     * @return String 操作状态
     */
    @Override
    public String setStringExAtMillisTimestamp(String key, long timeMillis, String value) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            String str = jedis.set(key, value);
            /*
            java时间戳转换成Unix timestamp
            Unix时间戳(Unix timestamp)，定义为从格林威治时间1970年01月01日00时00分00秒起至现在的总秒数。
            注意：Unix timestamp 得到的是秒数，java用getTime()得到的是毫秒！
            */
            long unixTimestamp = timeMillis / 1000;
            if (unixTimestamp > 0) {
                jedis.expireAt(key, unixTimestamp);
            }
            return str;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 对一个key设置过期时间
     *
     * @param key        要设置过期时间的key
     * @param timeMillis Java时间戳（毫秒数），如 date.getTime(), System.currentTimeMillis()
     * @return 1=设置成功，0=设置失败
     */
    @Override
    public Long expireAtMillisTimestamp(final String key, final long timeMillis) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            /*
            java时间戳转换成Unix timestamp
            Unix时间戳(Unix timestamp)，定义为从格林威治时间1970年01月01日00时00分00秒起至现在的总秒数。
            注意：Unix timestamp 得到的是秒数，java用getTime()得到的是毫秒！
            */
            long unixTimestamp = timeMillis / 1000;
            if (unixTimestamp > 0) {
                return jedis.expireAt(key, unixTimestamp);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 如果指定key不存在,设值,并设置有效时间(大于0的时间才会被设置)
     *
     * @param key
     * @param value
     * @param seconds 单位秒
     * @return 1设值成功, 0未设值,-1设值失败
     */
    @Override
    public long setnx(String key, String value, int seconds) {
        boolean isBroken = false;
        Jedis jedis = null;
        long result = -1;
        try {
            jedis = getJedis();
            result = jedis.setnx(key, value);
            if (result == 1 && seconds > 0) {
                jedis.expire(key, seconds);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return result;
    }

    /**
     * 查询符合匹配条件的键
     *
     * @param pattern
     * @return
     */
    @Override
    public Set<String> keys(String pattern) {
        boolean isBroken = false;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            return jedis.keys(pattern + "*");
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 删除符合匹配条件的键
     *
     * @param pattern
     */
    @Override
    public long delByPattern(String pattern) {
        boolean isBroken = false;
        Jedis jedis = null;
        try {
            jedis = getJedis();
            Set<String> keySet = jedis.keys(pattern + "*");
            if (keySet != null && keySet.size() > 0) {
                String[] keys = new String[keySet.size()];
                keySet.toArray(keys);
                return jedis.del(keys);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return 0L;
    }

    /**
     * 删除keys对应的记录,可以是多个key
     *
     * @param keys 要删除的key数组
     * @return 删除的记录数
     */
    @Override
    public long del(String... keys) {
        boolean isBroken = false;
        Jedis jedis = null;
        long result = 0;
        try {
            jedis = getJedis();
            return jedis.del(keys);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return result;
    }

    /**
     * 验证key 是否存在
     *
     * @param key
     * @return
     */
    @Override
    public boolean existKey(String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            return jedis.exists(key);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return false;
    }

    /**
     * 在instance出错时，必须调用returnBrokenResource返还给pool，否则下次通过getResource得到的instance的缓冲区可能还存在数据，出现问题
     *
     * @param jedis    Jedis连接对象
     * @param isBroken true: instance出错, false:instance正常
     */
    @Override
    public void release(Jedis jedis, boolean isBroken) {
        if (jedis != null) {
            if (isBroken) {
                mqJedisPool.returnBrokenResource(jedis);
            } else {
                mqJedisPool.returnResource(jedis);
            }
        }
    }

    /**
     * 添加有过期时间的记录
     *
     * @param strKey
     * @param seconds 过期时间，以秒为单位
     * @param obj
     */
    @Override
    public void setObjex(String strKey, int seconds, Object obj) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            String key = strKey;
            jedis = getJedis();
            byte[] value = serialize(obj);
            if (value != null) {
                jedis.setex(key.getBytes(), seconds, value);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
    }

    /**
     * cache对象
     * cache的实际key = obj类名 + "_" + key(参数)
     *
     * @param strKey - key
     * @param obj    - cache对象
     */
    @Override
    public void setObj(String strKey, Object obj) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            String key = obj.getClass().getSimpleName() + "_" + strKey;
            jedis = getJedis();
            byte[] value = serialize(obj);

            if (value != null) {
                jedis.set(key.getBytes(), value);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
    }

    /**
     * 取得cache对象
     *
     * @param strKey   - key
     * @param objClass - 对象Class
     * @return
     */
    @Override
    public Object getObj(String strKey, Class objClass) {
        Jedis jedis = null;
        Object returnObj = null;
        boolean isBroken = false;
        try {
            String key = objClass.getSimpleName() + "_" + strKey;
            jedis = getJedis();
            byte[] value = jedis.get(key.getBytes());
            if (value != null) {
                returnObj = unserialize(value);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return returnObj;
    }

    @Override
    public Object getObjex(String strKey, Class objClass) {
        Jedis jedis = null;
        Object returnObj = null;
        boolean isBroken = false;
        try {
            String key = strKey;
            jedis = getJedis();
            byte[] value = jedis.get(key.getBytes());

            if (value != null) {
                returnObj = unserialize(value);
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return returnObj;
    }

    /**
     * 删除cache对象
     *
     * @param strKey
     * @param objClass
     * @return
     */
    @Override
    public long delObj(String strKey, Class objClass) {
        boolean isBroken = false;
        Jedis jedis = null;
        long result = 0;
        try {
            String key = objClass.getSimpleName() + "_" + strKey;
            jedis = getJedis();
            return jedis.del(key.getBytes());
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }

        return result;
    }

    /**
     * 删除cache对象
     *
     * @param strKeys
     * @param objClass
     * @return
     */
    @Override
    public long delObjs(String[] strKeys, Class objClass) {
        boolean isBroken = false;
        Jedis jedis = null;
        long result = 0;
        try {
            byte[][] byteKeys = new byte[strKeys.length][];
            int cnt = 0;
            for (String strKey : strKeys) {
                String key = objClass.getSimpleName() + "_" + strKey;
                byteKeys[cnt] = key.getBytes();
                cnt++;
            }

            jedis = getJedis();
            return jedis.del(byteKeys);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }

        return result;
    }

    /**
     * 序列化对象
     *
     * @param object
     * @return
     */
    @Override
    public byte[] serialize(Object object) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;

        try {
            // 序列化
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            byte[] bytes = baos.toByteArray();

            return bytes;
        } catch (Exception ex) {
            LOG.error(ex.getLocalizedMessage(), ex);
            ex.printStackTrace();
        }

        return null;
    }

    /**
     * 反序列化
     *
     * @param bytes
     * @return
     */
    @Override
    public Object unserialize(byte[] bytes) {
        ByteArrayInputStream bais = null;

        try {
            // 反序列化
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);

            return ois.readObject();
        } catch (Exception ex) {
            LOG.error(ex.getLocalizedMessage(), ex);
            ex.printStackTrace();
        }

        return null;
    }

    /**
     * 把元素插入LIST的左侧（头部添加）
     *
     * @param key     LIST结构名字
     * @param strings String对象元素
     * @return 返回插入元素后，LIST中元素的总数
     */
    @Override
    public Long lpush(final String key, final String... strings) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.lpush(key, strings);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 把元素插入LIST的右侧（尾部添加）
     *
     * @param key     LIST结构名字
     * @param strings String对象元素
     * @return 返回插入元素后，LIST中元素的总数
     */
    @Override
    public Long rpush(final String key, final String... strings) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.rpush(key, strings);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 取出一个范围内的元素列表，取出所有：start=0, end=-1
     *
     * @param key   LIST结构名字
     * @param start 开始位置，从0开始
     * @param end   结束位置，-1表示最后一个元素
     * @return
     */
    @Override
    public List<String> lrange(final String key, final long start, final long end) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            List<String> result = jedis.lrange(key, start, end);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 获取LIST中元素个数
     *
     * @param key LIST结构名字
     * @return
     */
    @Override
    public Long llen(final String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.llen(key);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 设置HASH结构中的键值对
     *
     * @param key   HASH名字
     * @param field
     * @param value
     * @return
     */
    @Override
    public Long hset(final String key, final String field, final String value) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.hset(key, field, value);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 从HASH结构中获取数据
     *
     * @param key   HASH名字
     * @param field HASH中的键值对key
     * @return
     */
    @Override
    public String hget(final String key, final String field) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            String result = jedis.hget(key, field);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    @Override
    public Set<String> hkeys(final String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            return jedis.hkeys(key);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    @Override
    public List<String> hvals(final String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            return jedis.hvals(key);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 将HASH结构数据存入Redis
     *
     * @param key  HASH结构本身KEY
     * @param hash
     * @return
     */
    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            String result = jedis.hmset(key, hash);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 从HASH结构中一次获取多个值
     *
     * @param key    HASH名字
     * @param fields
     * @return
     */
    @Override
    public List<String> hmget(final String key, final String... fields) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            List<String> result = jedis.hmget(key, fields);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 获取HASH的全部key-value对
     *
     * @param key
     * @return
     */
    @Override
    public Map<String, String> hgetall(final String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Map<String, String> result = jedis.hgetAll(key);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 从HASH中删除键值对
     *
     * @param key    HASH名字
     * @param fields
     * @return
     */
    @Override
    public Long hdel(final String key, final String... fields) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.hdel(key, fields);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 将数据存入ZSET结构中，存在则更新
     *
     * @param key    ZSET名字
     * @param score
     * @param member
     * @return
     */
    @Override
    public Long zadd(final String key, final double score, final String member) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.zadd(key, score, member);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 批量存入ZSET结构中，存在则更新
     *
     * @param key          ZSET名字
     * @param scoreMembers
     * @return
     */
    @Override
    public Long zadd(final String key, final Map<String, Double> scoreMembers) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.zadd(key, scoreMembers);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 从ZSET中获取数据，获取score在min~max之间的所有member
     * (including elements with score equal to min or max)
     *
     * @param key ZSET名字
     * @param min SCORE最小值
     * @param max SCORE最大值
     * @return
     */
    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Set<String> result = jedis.zrangeByScore(key, min, max);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 从ZSET中获取数据，获取score在min~max之间的所有member
     * (including elements with score equal to min or max)
     *
     * @param key    ZSET名字
     * @param min    SCORE最小值
     * @param max    SCORE最大值
     * @param offset 分页开始位置
     * @param count  本次获取最大条数
     * @return
     */
    @Override
    public Set<String> zrangeByScore(final String key, final double min, final double max,
                                     final int offset, final int count) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Set<String> result = jedis.zrangeByScore(key, min, max, offset, count);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 获取ZSET全部元素，演示，获取全部：zrange key 0 -1
     *
     * @param key   ZSET名字
     * @param start 索引开始位置
     * @param end   索引结束位置
     * @return
     */
    @Override
    public Set<String> zrange(final String key, final long start, final long end) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Set<String> result = jedis.zrange(key, start, end);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 从ZSET中删除
     *
     * @param key     ZSET名字
     * @param members member名字
     * @return
     */
    @Override
    public Long zrem(final String key, final String... members) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.zrem(key, members);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 获取ZSET中某个元素的score，如果member不存在或key不存在，则返回null
     *
     * @param key    ZSET名字
     * @param member member名字
     * @return
     */
    @Override
    public Double zscore(final String key, final String member) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Double result = jedis.zscore(key, member);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 增加ZSET中member的score，不存在则直接添加
     *
     * @param key    ZSET名字
     * @param score  增量
     * @param member member名字
     * @return
     */
    @Override
    public Double zincrby(final String key, final double score, final String member) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Double result = jedis.zincrby(key, score, member);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 对一个key设置过期时间，到期后key将会被自动删除
     *
     * @param key     要设置过期时间的key
     * @param seconds 过期的时间，单位：秒
     * @return 1=设置成功，0=设置失败
     */
    @Override
    public Long expire(final String key, final int seconds) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.expire(key, seconds);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * 获取一个key还剩余的超时时间
     *
     * @param key Redis的key
     * @return -2:key不存在，-1:没有为key设置超时时间，>0:超时时间
     */
    @Override
    public Long ttl(final String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            Long result = jedis.ttl(key);
            return result;
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return null;
    }

    /**
     * flashall
     *
     * @return
     */
    @Override
    public void flashAll() {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            jedis.flushAll();
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
    }

    @Override
    public Long incr(String key) {
        Jedis jedis = null;
        boolean isBroken = false;
        try {
            jedis = getJedis();
            return jedis.incr(key);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            isBroken = true;
        } finally {
            release(jedis, isBroken);
        }
        return 0L;
    }


}
