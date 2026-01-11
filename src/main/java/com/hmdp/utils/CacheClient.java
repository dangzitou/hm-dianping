package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

@Component
public class CacheClient {
    private StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //线程池用于缓存重建
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 写入缓存
     * @param key key
     * @param value value
     * @param time 过期时间
     * @param unit 时间单位
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpireTime(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //尝试获取锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(flag);
    }
    //释放锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }


    /**
     * 解决缓存穿透
     * @param keyPrefix key前缀
     * @param id id
     * @param type 返回类型
     * @param dbFallback 数据库查询函数
     * @param time 缓存时间
     * @param unit 时间单位
     * @param <R> 返回类型泛型
     * @param <ID> id类型泛型
     * @return R
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //如果存在，直接返回
        if(StrUtil.isNotBlank(json)){//isNotBlank可以判断非空且非空字符串
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否是空值
        if(json != null){//既不是空字符串也不是null，说明是缓存的空值
            return null;
        }
        //不存在，查询数据库
        R r = dbFallback.apply(id);
        //不存在，返回错误
        if(r == null) {
            //将空值写入redis
            this.set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //存在，写入redis
        this.set(key, JSONUtil.toJsonStr(id), time, unit);
        //返回
        return r;
    }

    /**
     * 逻辑过期时间解决缓存击穿
     * @param keyPrefix key前缀
     * @param lockKeyPrefix 锁key前缀
     * @param id id
     * @param type 返回类型
     * @param dbFallback 数据库查询函数
     * @param time 缓存时间
     * @param unit 时间单位
     * @param <R> 返回类型泛型
     * @param <ID> id类型泛型
     * @return R
     */
    public <R, ID> R queryWithLogicalExpireTime(String keyPrefix, String lockKeyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //如果不存在，直接返回null，不需要考虑缓存穿透问题是因为逻辑过期只会缓存存在的数据
        if(StrUtil.isBlank(json)){
            return null;
        }
        //命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONUtil.parseObj(redisData.getData())), type);
        //判断是否过期
        LocalDateTime expireTime = redisData.getExpireTime();
        //未过期，直接返回店铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //过期，需要重建缓存
        //1.获取互斥锁
        String lockKey = lockKeyPrefix + id;
        boolean isLocked = tryLock(lockKey);
        //2.判断是否获取成功
        if(isLocked){
            //3.成功，开启独立线程缓存重建
            CACHE_REBUILD_EXECUTOR.execute(() -> {
                try {
                    //重建缓存
                    R r1 = dbFallback.apply(id);
                    //写入缓存
                    this.setWithLogicalExpireTime(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //返回商铺信息
        return r;
    }

    /**
     * 互斥锁解决缓存击穿
     * @param keyPrefix key前缀
     * @param lockKeyPrefix 锁key前缀
     * @param id id
     * @param type 返回类型
     * @param dbFallback 数据库查询函数
     * @param <R> 返回类型泛型
     * @param <ID> id类型泛型
     * @return R
     */
    public <R, ID> R queryWithMutex(String keyPrefix, String lockKeyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //如果存在，直接返回
        if(StrUtil.isNotBlank(json)){//isNotBlank可以判断非空且非空字符串
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否是空值
        if(json != null){//既不是空字符串也不是null，说明是缓存的空值
            return null;
        }
        //实现缓存重建
        //1.获取互斥锁
        String lockKey = lockKeyPrefix + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            //2.判断是否获取成功
            //失败则休眠重试
            if(!isLock){
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, lockKeyPrefix, id, type, dbFallback, time, unit);//递归
            }
            //3.成功，根据id查询数据库
            r = dbFallback.apply(id);
            //不存在，返回商铺不存在
            if(r == null){
                //将空值写入redis缓存，防止缓存穿透
                this.set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //模拟重建延时
            Thread.sleep(200);
            //存在，写入redis缓存
            this.set(key, JSONUtil.toJsonStr(r), time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            unlock(lockKey);
        }
        //返回
        return r;
    }

}
