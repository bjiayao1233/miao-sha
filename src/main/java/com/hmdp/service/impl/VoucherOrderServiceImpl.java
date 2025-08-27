package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.db.sql.Order;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;


    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new voucherOrderHandler());
    }



    private class voucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //获取消息队列中的订单信息  xreadgroup group g1 c1 count 1 block 200 streams stream.order
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    if (list == null || list.isEmpty()) {
                        //判断消息获取是否成功，获取失败，循环
                        continue;
                    } //成功，下单成功
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();

                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handlerVoucherOrder(voucherOrder);
                    //ack确认  sack
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());


                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    heandlependingList();
                }
            }
        }

        private void heandlependingList() {
            while (true) {
                try {
                    //获取pendingList中的订单信息  xreadgroup group g1 c1 count 1 block 200 streams 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    if (list == null) {
                        //判断消息获取是否成功，获取失败，pending-List没消息循环
                        break;
                    } //成功，下单成功
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();

                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handlerVoucherOrder(voucherOrder);
                    //ack确认  sack
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-List订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    /*//创建线程池
    private BlockingQueue<VoucherOrder> orderTask = new ArrayBlockingQueue<>(1024 * 1024);

    private class voucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTask.take();
                    //2.创建订单
                    handlerVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/
    private IVoucherOrderService proxy;

    private void handlerVoucherOrder(VoucherOrder voucherOrder) {
        Long id = voucherOrder.getUserId();
        //获取锁
        RLock lock = redissonClient.getLock("lock:order:" + id);
        boolean trylock = lock.tryLock();
        if (!trylock) {
            //获取锁失败
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }


    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT
                , Collections.emptyList()
                , voucherId.toString()
                , UserHolder.getUser().getId().toString()
                , String.valueOf(orderId));
        //判断结果是否为零，不为零，没有资格购买
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }
/*    @Override
    public Result seckillVoucher(Long voucherId) {
        //执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT
                , Collections.emptyList()
                , voucherId
                , UserHolder.getUser().getId().toString());
        //判断结果是否为零，不为零，没有资格购买
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //为零，有购买资格，把订单写入阻塞队列，数据库
        //todo 保存阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        //创建阻塞队列
        orderTask.add(voucherOrder);
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }*/

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始或结束
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }

        //3.判断库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        //一人一单
        Long id = UserHolder.getUser().getId();
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + id, stringRedisTemplate);
        //获取锁
        RLock lock = redissonClient.getLock("lock:order:" + id);
        boolean trylock = lock.tryLock();
        if (!trylock) {//获取锁失败
            return Result.fail("一个人只能买一单");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }


        *//*synchronized (id.toString().intern()) {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }*//*
    }*/

    /**
     * 创建订单
     *
     * @param voucherOrder
     */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //4.一人一单
        Long id = voucherOrder.getUserId();
        //查询订单
        Integer count = query().eq("user_id", id).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("一人一单");
            return;
        }
        //4..扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)//乐观锁
                .update();//
        if (!success) {
            log.error("库存不足");
            return;
        }
        //5.创建订单   订单id  用户id  代金券id
        save(voucherOrder);
        //6.返回订饭id
        //结束

    }
}
