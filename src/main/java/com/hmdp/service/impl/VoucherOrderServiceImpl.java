package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
//    private final BlockingQueue<VoucherOrder> orderTasks= new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable {
        String queryName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try{
                    // 1.获取消息队列中的订单 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queryName, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 如果失败，说明没有消息，继续循环
                        continue;
                    }
                    // 3.获取成功，解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 4.保存订单
                    handlerVoucherOrder(voucherOrder);
                    // 5.ack确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queryName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queryName, ReadOffset.from("0"))
                    );
                    // 2.判断消息获取是否成功
                    if (list == null || list.isEmpty()) {
                        // 如果失败，说明pending-list中没有异常消息，结束循环
                        break;
                    }
                    // 3.获取成功，解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 4.保存订单
                    handlerVoucherOrder(voucherOrder);
                    // 5.ack确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queryName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        private void handlerVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            // 1.创建锁对象
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            // 2.获取锁
            boolean isLock = lock.tryLock();
            // 3.判断是否获取成功
            if (!isLock) {
                // 4.获取锁失败，返回，理论上不会的
                log.error("不允许重复下单！");
                return;
            }
            try {
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));
        if(result == null) {
            return Result.fail("seckill.lua异常！");
        }
        // 2.判断结果是否为0
        int r = result.intValue();
        if(r != 0) {
            // 2.1.不为0.代表没有购买资格
            return Result.fail(r == 1 ? "库存不足！" : "不能重复下单！");
        }
        // 3.返回订单id
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 1.一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 2.查询订单
        long count = lambdaQuery().eq(VoucherOrder::getUserId, userId).eq(VoucherOrder::getVoucherId, voucherId).count();
        // 3.判断是否存在
        if(count > 0) {
            log.error("用户已经购买过该优惠券！");
            return;
        }
        // 4.扣减库存
        boolean update = seckillVoucherService.lambdaUpdate().setSql("stock=stock-1")  // ??怎么防止拼接
                .eq(SeckillVoucher::getVoucherId, voucherId).gt(SeckillVoucher::getStock, 0).update();
        if(!update) {
            log.error("库存不足！");
            return;
        }
        // 5.将订单写入数据库
        save(voucherOrder);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.执行lua脚本
//        Long userId = UserHolder.getUser().getId();
//        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
//                voucherId.toString(), userId.toString());
//        if(result == null) {
//            return Result.fail("seckill.lua异常！");
//        }
//        // 2.判断结果是否为0
//        int r = result.intValue();
//        if(r != 0) {
//            // 2.1.不为0.代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足！" : "不能重复下单！");
//        }
//        // 2.2.为0，有购买资格，把下单信息保存到阻塞队列
//        // 6.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 6.1.订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 6.2.用户id
//        voucherOrder.setUserId(userId);
//        // 6.3.代金券id
//        voucherOrder.setVoucherId(voucherId);
//        // 获取代理对象（事务）
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        // 添加到阻塞队列
//        orderTasks.add(voucherOrder);
//        // 3.返回订单id
//        return Result.ok(orderId);
//    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始或结束
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始！");
//        }
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束！");
//        }
//        // 3.判断库存是否充足
//        if(voucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//        Long userId = UserHolder.getUser().getId();
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            return Result.fail("不允许重复下单！");
//        }
//        try {
//            // 获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
//    }



}
