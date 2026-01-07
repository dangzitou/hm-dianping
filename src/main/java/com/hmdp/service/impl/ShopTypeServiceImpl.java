package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_TYPE_LIST;

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
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 查询商铺类型列表
     * @return 商铺类型列表
     */
    @Override
    public Result queryTypeList() {
        String key = CACHE_TYPE_LIST;
        //1.从redis中查询商铺类型缓存
        List<String> typeList = stringRedisTemplate.opsForList().range(key, 0, -1);
        //2.如果存在，直接返回
        if(typeList != null && !typeList.isEmpty()){
            //将typeList转换为List<ShopType>并返回
            List<ShopType> shopTypes = typeList.stream()
                    .map(json -> JSONUtil.toBean(json, ShopType.class))
                    .collect(Collectors.toList());
            return Result.ok(shopTypes);
        }
        //3.不存在，查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        //4.不存在，返回商铺类型不存在
        if(shopTypeList == null || shopTypeList.isEmpty()){
            return Result.fail("商铺类型不存在");
        }
        //5.存在，写入redis缓存
        List<String> jsonList = shopTypeList.stream()
                .map(JSONUtil::toJsonStr)
                .collect(Collectors.toList());
        stringRedisTemplate.opsForList().rightPushAll(key, jsonList);
        stringRedisTemplate.expire(key, CACHE_SHOP_TTL, TimeUnit.HOURS);
        //6.返回
        return Result.ok(shopTypeList);
    }
}
