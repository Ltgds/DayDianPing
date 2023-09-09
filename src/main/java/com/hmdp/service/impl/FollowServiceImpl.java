package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    /**
     * 关注 / 取关
     * @param followUserId  被关注用户id
     * @param isFollow      是否关注
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //1.获取登录用户(因为是要对登录的用户判断是否 要去关注followUserId这个用户
        Long userId = UserHolder.getUser().getId();

        String key = "follows:" + userId;

        //1.判断到底是关注还是取关
        if (isFollow) {
            //2.关注,新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            //只有在数据库操作成功后才可以
            if (isSuccess) {
                //将关注用户的id,放入redis的set集合中 sadd userId followUserId
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            //3.取关,删除数据  delete tb_follow where user_id = ? and follow_user_id = ?
            QueryWrapper<Follow> wrapper = new QueryWrapper<Follow>()
                    .eq("user_id", userId)
                    .eq("follow_user_id", followUserId);
            boolean isSuccess = remove(wrapper);

            if (isSuccess) {
                //移除关注的用户id从Redis集合中移除
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    /**
     * 判断用户是否关注
     * @param followUserId  被关注用户id
     * @return
     */
    @Override
    public Result isFollow(Long followUserId) {
        //1.获取登录用户id
        Long userId = UserHolder.getUser().getId();

        //1.查询是否关注 select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        //  使用count(*)是因为没必要知道具体内容,只需要知道是否查到即可
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();

        //2.判断是否关注 >0表示关注
        return Result.ok(count > 0);
    }

    /**
     * 实现查看共同关注
     * @param id 博主的id
     * @return
     */
    @Override
    public Result followCommons(Long id) {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();

        String key = "follows:" + userId; //登录用户的key
        String key2 = "follows:" + id; //目标用户的key

        //2.求交集
        //得到他俩交集的id
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);

        if (intersect == null || intersect.isEmpty()) {
            //无交集
            return Result.ok(Collections.emptyList());
        }

        //3.解析id集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());

        //4.查询用户
        List<User> users = userService.listByIds(ids);

        List<UserDTO> userDTOS = users.stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
