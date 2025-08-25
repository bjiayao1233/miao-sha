package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    /**
     * 时间戳起始点
     */
    private static final long BEGIN_TIMESTAMP = 1577836800L;//2020-01-01 00:00:00
    private static final int COUNT_BITs = 32;


    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyprefix) {
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecound = now.toEpochSecond(ZoneOffset.UTC);//获取秒数
        long timestamp = nowSecound - BEGIN_TIMESTAMP;//获取秒数
        //2.生成序列号
        String data = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyprefix + ":" + data);//自增长
        //3.时间戳+序列号（各32位）  拼接并返回
        return timestamp << COUNT_BITs | count;
    }


}
