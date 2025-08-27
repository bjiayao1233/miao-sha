package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.apache.ibatis.annotations.Delete;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        //判单是否关注还是取关
        if (isFollow) {
            //关注
            Follow follow = new Follow();
            follow.setUserId(userId);//当前用户id
            follow.setFollowUserId(followUserId);//关注用户id
            boolean isSuccess = save(follow);
            if (isSuccess) {

                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }

        } else {
            //取关
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", followUserId));
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isfollow(Long followUserId) {
        //判断是否关注
        Integer count = query().eq("user_id", UserHolder.getUser().getId()).eq("follow_user_id", followUserId).count();

        return Result.ok(count > 0);

    }

    @Override
    public Result followCommons(Long id) {
        //查询共同关
        Long userId = UserHolder.getUser().getId();

    }
}
