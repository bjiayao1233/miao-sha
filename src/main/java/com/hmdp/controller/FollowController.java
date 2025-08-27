package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {

    @Autowired
    private IFollowService followService;

    /**
     * 关注或取关
     *
     * @param id
     * @param isFollow
     * @return
     */
    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long id, @PathVariable("isFollow") Boolean isFollow) {
        return followService.follow(id, isFollow);
    }

    /**
     * 判断是否关注
     *
     * @param id
     * @return
     */
    @GetMapping("/or/not/{id}")
    public Result follow(@PathVariable("id") Long id) {
        return followService.isfollow(id);
    }

    @GetMapping("/common/{id}")
    public Result followCommons(@PathVariable("id") Long id) {
        return followService.followCommons(id);
    }


}
