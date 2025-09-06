package com.basic.im.api.controller;

import com.basic.im.api.AbstractController;
import com.basic.im.api.utils.IpAccessObjectUtil;
import com.basic.im.company.service.SignManager;
import com.basic.im.pay.service.impl.ExtendManagerImpl;
import com.basic.im.user.entity.UserSign;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @title: ExtendController
 * @fileName: ExtendController.java
 * @author:
 * @date:2018年10月26日 下午3:04:48
 * @description:扩展功能接口
 */

@RestController
@RequestMapping("/extend")
public class ExtendController extends AbstractController {
    private static Logger logger = LoggerFactory.getLogger(ExtendController.class);

    @Autowired
    private SignManager signManager;

    @Autowired
    private ExtendManagerImpl extendManager;


    public Date getNestDayZeroTime(Date date, int days){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH)+days, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND,0);
        Date newDate = calendar.getTime();
        return newDate;
    }

    /**
     * 签到功能
     * @param device 设备ID
     * @param request 请求参数（免写）
     * @param userId 用户ID
     * @return
     */
    @RequestMapping("signIn")
    @ResponseBody
    public JSONMessage signIn(String device, HttpServletRequest request, @RequestParam(defaultValue="") Integer userId) throws Exception {
        String signIp = IpAccessObjectUtil.getIpAddress(request);
        try{
            Date newDate = this.getNestDayZeroTime(new Date(),1);
            Long timeOut = (newDate.getTime()-System.currentTimeMillis()) / 1000;

            boolean ipCheckStatus = signManager.saveUserSign(userId, signIp, device, timeOut);
            if (!ipCheckStatus){
                return JSONMessage.failure("今天已经签到过，明天再来");
            }
            Float status = extendManager.userSign(userId,device,signIp);
            if (status >= 0f){
                Map<String,Object> result = extendManager.getUserSignDateByWeek(userId);
                result.put("msg","签到成功");
                return JSONMessage.success(result);
            }
        }catch (Exception e){
            signManager.delUserSign(userId, signIp, device);
            return JSONMessage.failure(e.getMessage());
        }
        signManager.delUserSign(userId, signIp, device);
        return JSONMessage.failure("签到失败");
    }

    /**
     * 获取用户7天内签到情况
     * @param userId 用户ID
     * @return
     */
    @RequestMapping("getUserSignDateByWeek")
    @ResponseBody
    public JSONMessage getUserSignDateByWeek(@RequestParam(defaultValue="") Integer userId) throws ParseException {
        Map<String,Object> signDate = extendManager.getUserSignDateByWeek(userId);
        return JSONMessage.success(signDate);
    }

    /**
     * 获取用户某月内签到情况
     * @param userId 用户ID
     * @param monthStr 年月参数（格式：2019-02）
     * @return
     */
    @RequestMapping("getUserSignDateByMonth")
    @ResponseBody
    public JSONMessage getUserSignDateByMonth(@RequestParam(defaultValue="") Integer userId,String monthStr) throws ParseException {
        List<UserSign> signDate = extendManager.getUserSignDateByMonth(userId,monthStr);
        return JSONMessage.success(signDate);
    }
}

