package com.hmdp.utils;

import cn.hutool.core.lang.TypeReference;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * ClassName: CacheClient
 * Package: com.hmdp.utils
 * Description:
 *
 * @Author: Jerry
 * @Date: 2023/8/20
 */

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <ID, R> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                          Long time, TimeUnit unit) {
        // 1.从redis查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        } else if ("".equals(json)) {
            // 已知数据库中也不存在
            return null;
        }
        // 4.不存在，查询数据库
        R r = dbFallback.apply(id);
        // 5.不存在，返回错误信息
        if(r == null) {
            // 将null写入redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.存在，写入redis
        this.set(key, r, time, unit);
        // 7.返回
        return r;
    }

    public <ID, R> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                            Long time, TimeUnit unit) {
        // 1.从redis查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if(StrUtil.isBlank(json)) {
            // 3.不存在，返回null
            return null;
        }
        // 4.存在，判断是否过期
        // 先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
//        R r = type.cast(redisData.getData());
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.未过期，返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }
        // 6.过期，尝试获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 7.判断是否获取成功
        if(isLock) {
            // 8.获取成功，创建新线程，实现缓存重建
            try {
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    R r1 = dbFallback.apply(id);
                    // 5.不存在，将""写入redis
                    if(r1 == null) {
                        stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                    } else {
                        // 6.存在，封装逻辑过期时间并写入redis
                        this.setWithLogicalExpire(key, r1, time, unit);
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                // 释放锁
                unLock(lockKey);
            }
        }
        // 9.返回已过期的商铺信息
        return r;
    }

    private boolean tryLock(String key) {
        Boolean setFlag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(setFlag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

}
