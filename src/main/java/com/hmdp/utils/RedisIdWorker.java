package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    //设置时间戳初始值
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    //设置序列号的位数
    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    //使用构造函数注入
    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix) {
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        //用当前时间-初始时间,得到时间戳
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        //2.生成序列号
        //获取当前日期,精确到天
        //  - 避免超过上限
        //  - 方便做统计
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        //自增长
        long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + date);

        //3.拼接并返回
        //需要以数字的形式做拼接,用到位运算
        //需要将 时间戳 向左移动32位
        //移动后,后面的32位都为0,使用或运算来将count填充到后面
        return timestamp << COUNT_BITS | count;
    }

    public static void main(String[] args) {
        //设置初始值
        LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println(second);
    }
}
