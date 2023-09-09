package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
     * @param key
     * @param value
     * @param time
     * @param unit
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
     * 逻辑过期永久有效，所以没有时间
     * @param key
     * @param value
     * @param time      时间
     * @param unit      时间单位
     */
    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }


    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     * @param keyPrefix     key的前缀
     * @param id            keyPrefix+id拼接
     * @param type
     * @param dbFallback    查询数据库的方法
     * @param time          缓存持续时间
     * @param unit
     * @return
     * @param <R>           返回的类型
     * @param <ID>          id的类型
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type
            , Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断redis中缓存是否存在
        if (StrUtil.isNotBlank(json)) {
            //3.存在，反序列化为R的类型，并返回
            return JSONUtil.toBean(json, type);
        }

        //判断命中的是否是空值
        if (json != null) { //若不为空,则说明是一个 "",已经是缓存穿透了
            //返回一个错误信息
            return null;
        }

        //不存在,根据id查询数据库
        //由于自己是不知道该查询什么的,因此由方法调用这传递这段逻辑
        //有参有返回值的用 Function<ID,R>
        R r = dbFallback.apply(id);

        //数据库中也没有数据
        if (r == null) {
            //将""写入redis中,防止以后再访问数据库
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错我信息
            return null;
        }

        //若数据库中有数据,将数据写入redis中
        this.set(key, r, time, unit);

        return r;
    }


    // 实现一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    /**
     * 获取锁
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        //设置锁,过期时间为10秒
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key
     */
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <ID>
     */
    public <R, ID> R queryWithLogicExpire(String keyPrefix, ID id, Class<R> type
            , Function<ID, R> dbFallback, Long time, TimeUnit unit) {

        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断redis缓存是否存在
        if (StrUtil.isBlank(json)) {
            //若为空,直接返回
            return null;
        }

        //3.若不为空,做反序列化操作,将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //4.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期,直接返回店铺信息
            return r;
        }

        //已过期,需要缓存重建
        //5.缓存重建
        //获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //判断获取锁是否成功
        if (isLock) {
            //6.成功,开启独立线程,实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //查询数据库
                    R value = dbFallback.apply(id);
                    //写入redis,带上逻辑封装的写
                    this.setWithLogicExpire(key, value, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //若锁获取失败,返回过期的信息
        return r;
    }
}
