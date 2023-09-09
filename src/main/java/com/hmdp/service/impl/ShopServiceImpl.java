package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import io.lettuce.core.api.sync.RedisGeoCommands;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询Redis中的缓存
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {

        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class
                , this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicExpire(id);

        Shop shop = cacheClient.queryWithLogicExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class
                , this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);


        if (shop == null) {
            return Result.fail("店铺不存在");
        }


        //------------------------------------
//        //1.从redis查询商铺缓存
//        //  cache:shop:id
//        String key = RedisConstants.CACHE_SHOP_KEY + id;
//        //查询缓存信息是否存在
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//        //2.判断是否存在
//        /**
//         * StrUtil.isBlank()方法返回false的情况有三种
//         * 1. 参数为null
//         * 2. 参数为"",即空串
//         * 3. 参数为制表换行符等（不考虑）
//         */
//        if (!StrUtil.isBlank(shopJson)) {
//            //3.存在,直接返回缓存信息
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
//        }
//
//        //判断命中的是否为Null
//        //经过上面的过滤,剩下的只有两种情况：value=""或null
//        //如果value!=null,则value为"",此时缓存命中的就是空对象
//        if (shopJson != null) {
//            //返回错误信息
//            return Result.fail("店铺不存在");
//        }
//
//        //4.不存在,根据id查询数据库
//        Shop shop = getById(id);
//
//        //5.不存在,返回错误
//        if (shop == null) {
//            //将空字符串写入redis,值设置为"",设置过期时间为2分钟
//            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
//            //返回错误信息
//            return Result.fail("店铺不存在");
//        }
//
//        //3.存在,写入redis
//        //  设置过期时间为30分钟
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop)
//                , RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //7.返回
        return Result.ok(shop);
    }

    /**
     * 封装 互斥锁 代码
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) {
        //1.从redis查询商铺缓存
        //  cache:shop:id
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //查询缓存信息是否存在
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否存在
        /**
         * StrUtil.isBlank()方法返回false的情况有三种
         * 1. 参数为null
         * 2. 参数为"",即空串
         * 3. 参数为制表换行符等（不考虑）
         */
        if (!StrUtil.isBlank(shopJson)) {
            //3.存在,直接返回缓存信息
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //判断命中的是否为Null
        //经过上面的过滤,剩下的只有两种情况：value=""或null
        //如果value!=null,则value为"",此时缓存命中的就是空对象
        if (shopJson != null) {
            //返回错误信息
            return null;
        }

        // *********************************
        // 实现 互斥锁  缓存重建
        // *********************************
        // 4.1获取互斥锁
        String lockKey = "lock:shop:" + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2判断是否获取成功
            if (!isLock) {
                // 4.3失败,则休眠并重试
                Thread.sleep(50);
                //重试
                return queryWithMutex(id);
            }

            // 4.4获取锁成功,再次检查Redis缓存是否存在,如果存在则无需重建缓存
            //根据id查询数据库
            shop = getById(id);

            //5.不存在,返回错误
            if (shop == null) {
                //缓存击穿问题
                //将空字符串写入redis,值设置为"",设置过期时间为2分钟
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }

            //6.存在,写入redis
            //  设置过期时间为30分钟
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop)
                    , RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放Redis锁
            unLock(lockKey);
        }

        //8.返回
        return shop;
    }

    /**
     * 封装 缓存穿透 代码
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        //1.从redis查询商铺缓存
        //  cache:shop:id
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //查询缓存信息是否存在
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否存在
        /**
         * StrUtil.isBlank()方法返回false的情况有三种
         * 1. 参数为null
         * 2. 参数为"",即空串
         * 3. 参数为制表换行符等（不考虑）
         */
        if (!StrUtil.isBlank(shopJson)) {
            //3.存在,直接返回缓存信息
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //判断命中的是否为Null
        //经过上面的过滤,剩下的只有两种情况：value=""或null
        //如果value!=null,则value为"",此时缓存命中的就是空对象
        if (shopJson != null) {
            //返回错误信息
            return null;
        }

        //4.不存在,根据id查询数据库
        Shop shop = getById(id);

        //5.不存在,返回错误
        if (shop == null) {
            //将空字符串写入redis,值设置为"",设置过期时间为2分钟
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }

        //3.存在,写入redis
        //  设置过期时间为30分钟
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop)
                , RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //7.返回
        return shop;
    }

    // 实现一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 逻辑过期 解决缓存击穿
     * @param id
     * @return
     */
    public Shop queryWithLogicExpire(Long id) {
        //1.从redis查询商铺缓存
        //  cache:shop:id
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //查询缓存信息是否存在
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断是否命中缓存
        if (StrUtil.isBlank(shopJson)) {
            //3.未命中,返回空
            return null;
        }

        //4.命中,需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5.判断缓存是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期,直接返回店铺信息
            return shop;
        }
        //已过期,需要缓存重建
        //6.缓存重建
        //获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //判断获取锁是否成功
        if (isLock) {
            //成功,使用独立线程,实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    this.saveShopToRedis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //返回过期商铺信息
        return shop;
    }



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
     * 设置逻辑过期时间
     * @param id
     * @param expireSeconds 过期时间
     */
    public void saveShopToRedis(Long id, Long expireSeconds) {
        //1.查询店铺数据
        Shop shop = getById(id);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id
                , JSONUtil.toJsonStr(redisData));
    }


    /**
     * 更新
     * @param shop
     * @return
     */
    @Override
    @Transactional //添加事务,当不通过则回滚
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);

        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }

//    @Override
//    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
//        //1.判断是否需要根据坐标来查询
//        if (x == null || y ==null) {
//            //不需要坐标查询,按数据库查
//            // 根据类型分页查询
//            Page<Shop> page = query()
//                    .eq("type_id", typeId)
//                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
//            // 返回数据
//            return Result.ok(page.getRecords());
//        }
//
//        //2.计算分页参数
//        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE; //5,每次查5条
//        int end = current * SystemConstants.DEFAULT_PAGE_SIZE; //结束
//
//        //3.查询redis,按照距离排序,分页. shopId, distance
//        String key = RedisConstants.SHOP_GEO_KEY + typeId;
//        stringRedisTemplate.opsForGeo() //GEOSEARCH bylocal x y byRadius 10 withDistance
//                .search(
//                        key,
//                        GeoReference.fromCoordinate(x, y),
//                        new Distance(5000),
//                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().
//                                limit(end)
//                )
//
//        //4.解析出id
//
//        //5,根据id查询shop
//
//
//    }
}
