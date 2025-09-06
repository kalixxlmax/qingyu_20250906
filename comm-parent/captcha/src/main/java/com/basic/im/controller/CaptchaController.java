/*
 *Copyright © 2018 anji-plus
 *安吉加加信息技术有限公司
 *http://www.anji-plus.com
 *All rights reserved.
 */
package com.basic.im.controller;

import com.alibaba.fastjson.JSONObject;
import com.anji.captcha.model.common.ResponseModel;
import com.anji.captcha.model.vo.CaptchaVO;
import com.anji.captcha.service.CaptchaService;
import com.anji.captcha.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/captcha")
public class CaptchaController {

    @Autowired
    private CaptchaService captchaService;

    @RequestMapping("/get")
    public ResponseModel get(@RequestBody CaptchaVO captchaVO) {
        // 默认滑块
        if (StringUtils.isEmpty(captchaVO.getCaptchaType())){
            captchaVO.setCaptchaType("blockPuzzle");
        }
        return captchaService.get(captchaVO);
    }

    @PostMapping("/check")
    public ResponseModel check(@RequestBody CaptchaVO captchaVO) {
        return captchaService.check(captchaVO);
    }

    @PostMapping("/verify")
    public ResponseModel verify(@RequestBody CaptchaVO captchaVO) {
        return captchaService.verification(captchaVO);
    }

}
