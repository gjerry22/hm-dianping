package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

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

    @Override
    public Result queryList() {
        // 1.从redis查询商铺缓存
        String shopTypeKey = RedisConstants.CACHE_SHOPTYPE_KEY;
        Set<String> keys = stringRedisTemplate.keys(shopTypeKey + "*");
        // 2.判断是否存在
        if(keys != null && !keys.isEmpty()) {
            // 3.存在，直接返回
            List<String> shopTypeJsonList = stringRedisTemplate.opsForValue().multiGet(keys);
            // 应该肯定为ture，保险起见加上判断
            if (shopTypeJsonList != null && !shopTypeJsonList.isEmpty()) {
                List<ShopType> shopTypeList = new ArrayList<>(shopTypeJsonList.size());
                for(String shopTypeJson : shopTypeJsonList) {
                    ShopType shopType = JSONUtil.toBean(shopTypeJson, ShopType.class);
                    shopTypeList.add(shopType);
                }
                shopTypeList.sort(Comparator.comparingInt(ShopType::getSort));
                return Result.ok(shopTypeList);
            }
        }
        // 4.不存在，查询数据库
        List<ShopType> shopTypeList = list();
        // 5.不存在，返回错误
        if(shopTypeList == null || shopTypeList.isEmpty()) {
            return Result.fail("店铺类型为空！");
        }
        // 6.存在，写入redis
        for (ShopType shopType: shopTypeList) {
            stringRedisTemplate.opsForValue().set(shopTypeKey + shopType.getId(), JSONUtil.toJsonStr(shopType));
        }
        // 7.返回
        shopTypeList.sort(Comparator.comparingInt(ShopType::getSort));
        return Result.ok(shopTypeList);
    }
}
