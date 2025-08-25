package com.hmdp.utils;



public interface Ilock {


    /**
     * 尝试获取锁
     *
     * @param timeoutsec
     * @return
     */
    boolean trylock(Long timeoutsec);


    void unlock();
}
