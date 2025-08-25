package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.aspectj.apache.bcel.util.ClassPath;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.swing.*;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@AllArgsConstructor
public class SimpleRedisLock implements Ilock {

    private String name;

    private StringRedisTemplate stringRedisTemplate;


    private static final String key_prefix = "lock:";
    private static final String id_prefix = UUID.randomUUID().toString(true) + "-";
    private static final DefaultRedisScript<Long> UN_LOCK_SCRIPT;

    static {
        UN_LOCK_SCRIPT = new DefaultRedisScript<>();
        UN_LOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UN_LOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean trylock(Long timeoutsec) {

        String threadid = id_prefix + Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(key_prefix + name, threadid, timeoutsec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        stringRedisTemplate.execute(UN_LOCK_SCRIPT
                , Collections.singletonList(key_prefix + name)
                , id_prefix + Thread.currentThread().getId());
    }

    /*@Override
    public void unlock() {
        String threadid = id_prefix + Thread.currentThread().getId();

        String id = stringRedisTemplate.opsForValue().get(key_prefix + name);
        if (threadid.equals(id)) {
            //释放锁
            stringRedisTemplate.delete(key_prefix + name);
        }
    }*/
}
