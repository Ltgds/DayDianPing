package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 给首页数据添加缓存
     * @return
     */
    @Override
    public Result queryByList() {

        //1.从redis中查询首页列表缓存
        String shopType = stringRedisTemplate.opsForValue().get("cache:shopType:list");

        //2.判断Redis中是否存在
        if (StrUtil.isNotBlank(shopType)) {
            //3.存在,直接返回
            List<ShopType> shopTypes = JSONUtil.toList(shopType, ShopType.class);
            return Result.ok(shopTypes);
        }

        //4.不存在,查询数据库中首页列表信息
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        //5.判断数据库中是否存在
        if (shopTypes == null) {
            //6.若不存在,返回错误
            return Result.fail("首页列表不存在");
        }

        //7.若存在,写入redis
        stringRedisTemplate.opsForValue().set("cache:shopType:list", JSONUtil.toJsonStr(shopTypes));

        return Result.ok(shopTypes);
    }

    /**
     * 使用List存储
     * @return
     */
    @Override
    public Result getByIconList() {
        //1.在redis中查询
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        //  range() 中-1表示最后一位
        //  shopTypeList中存放的数据是[{...},{...},...]一个列表中有一个个json对象
        List<String> shopTypeList = stringRedisTemplate.opsForList().range(key, 0, -1);

        //2.判断是否缓存了
        if (!shopTypeList.isEmpty()) {
            //3.若缓存了,返回
            ArrayList<ShopType> typeList = new ArrayList<>();
            for (String s : shopTypeList) {
                //将String逐个转为ShopType对象
                ShopType shopType = JSONUtil.toBean(s, ShopType.class);
                typeList.add(shopType);
            }
            return Result.ok(typeList);
        }

        //4.若缓存中没有,查询数据库
        //根据ShopType对象的sort属性排序后存入typeList
        List<ShopType> typeList = query().orderByAsc("sort").list();

        //5.判断数据库中是否存在
        if (typeList.isEmpty()) {
            return Result.fail("不存在分类");
        }

        //6.存在则添加进缓存
        stringRedisTemplate.opsForList().rightPushAll(key, shopTypeList);
        return Result.ok(typeList);
    }
}
