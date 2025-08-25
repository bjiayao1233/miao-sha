package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Component
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryShopType() {
        //1.从redis中查询缓存
        String s = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        //2.存在，直接返回
        if (StrUtil.isNotBlank(s)) {
            List<ShopType> list = JSON.parseArray(s, ShopType.class);
            return Result.ok(list);
        }
        //3.不存在，查询数据库
        List<ShopType> typeList = query().list();
        //4.数据库不存在，返回错误404
        if (typeList == null) {
            return Result.fail("店铺类型不存在");
        }
        //5.数据库存在，写入redis，
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, JSON.toJSONString(typeList));
        //6.返回
        return Result.ok(typeList);
    }
}
