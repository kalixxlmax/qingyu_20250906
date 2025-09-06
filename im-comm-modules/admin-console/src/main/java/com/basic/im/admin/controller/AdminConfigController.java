package com.basic.im.admin.controller;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.utils.LoginPassword;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.impl.AuthKeysServiceImpl;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.basic.im.admin.entity.TotalConfig;
import com.basic.im.admin.jedis.AdminRedisRepository;
import com.basic.im.admin.service.AdminManager;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.HttpUtil;
import com.basic.im.entity.*;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.RoomManager;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.service.StickDialogService;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.MoneyUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.StringUtil;
import io.swagger.annotations.ApiOperation;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.*;

/**
 * @ClassName AdminConfigController
 * @Author xie yuan yang
 * @date 2020.11.11 09:40
 * @Description 管理后台 配置相关接口
 */
@ApiIgnore
@RestController
@RequestMapping("/console")
public class AdminConfigController {

    @Autowired
    private AdminManager adminManager;

    @Autowired
    private AdminRedisRepository adminRedisRepository;

    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private AuthKeysServiceImpl authKeysService;


    @RequestMapping(value = "/config")
    public JSONMessage getConfig() {
        Config config = SKBeanUtils.getSystemConfig();
        if (ObjectUtil.isNull(config.getSystemApiConfig())){
            config.setSystemApiConfig(adminManager.resetSystemApiConfig());
            adminManager.setConfig(config);
        }
        if (ObjectUtil.isNull(config.getSystemApiConfig().getRequestApiList())){
            config.getSystemApiConfig().setRequestApiList(new ArrayList<String>());
        }
        config.getSystemApiConfig().setPrivateKey("");
        config.setDistance(ConstantUtil.getAppDefDistance());
        return JSONMessage.success(config);
    }


    /**
     * 设置服务端全局配置
     **/
    @RequestMapping(value = "/config/set", method = RequestMethod.POST)
    public JSONMessage setConfig(@ModelAttribute Config config) {
        try {
            //最大群人数不得超过10000
            if (config.getMaxCrowdNumber() > 10000){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.MaxCrowdNumber);
            }
            if (config.getMaxCrowdNumber() < config.getMaxUserSize()){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.PassRoomMax);
            }
            adminManager.setConfig(config);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }


    /**
     *  设置服务端全局配置
     **/
    @RequestMapping(value = "/config/running/set", method = RequestMethod.POST)
    public JSONMessage setRunningConfig(@ModelAttribute Config config, String requestApiList) {
        try {
            //最大群人数不得超过10000
            if (config.getMaxCrowdNumber() > 10000){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.MaxCrowdNumber);
            }
            if (config.getMaxCrowdNumber() < SKBeanUtils.getImCoreService().getConfig().getMaxUserSize()){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.PassRoomMax);
            }
            adminManager.setRunningConfig(config, requestApiList);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
}

    /**
     * 设置服务端用户默认配置
     **/
    @RequestMapping(value = "/config/default/user/set", method = RequestMethod.POST)
    public JSONMessage setDeafultUserConfig(@ModelAttribute Config config) {
        try {
            adminManager.setUserDefaultConfig(config);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 设置服务端建立群组配置
     **/
    @RequestMapping(value = "/config/default/room/set")
    public JSONMessage setDefaultRoomConfig(@ModelAttribute Config config) {
        try {
            Config config_ = SKBeanUtils.getSystemConfig();
            if (config_.getMaxCrowdNumber() < config.getMaxUserSize()){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.PassRoomMax);
            }
            adminManager.setRoomDefaultConfig(config);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * 设置服务端公告配置
     **/
    @RequestMapping(value = "/config/default/roomNotice/set")
    public JSONMessage setDefaultRoomConfig(String roomNotice) {
        try {
            Config config = SKBeanUtils.getSystemConfig();
            config.setRoomNotice(roomNotice);
            adminManager.setConfig(config);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }


    /**
     * 设置客户端配置
     **/
    @RequestMapping(value = "/clientConfig/set")
    public JSONMessage setClientConfig(@ModelAttribute ClientConfig clientConfig) throws Exception{
        try {
            if (!StringUtil.isEmpty(clientConfig.getWebsite())){
                if (StringUtil.isEmpty(clientConfig.getApiUrl())){
                    return JSONMessage.failure("请输入接口URL");
                }
                List<String> domain = HttpUtil.getUrlDomain(clientConfig.getWebsite());
                List<String> domain_ = HttpUtil.getUrlDomain(clientConfig.getApiUrl());
                if (domain_.size() < KConstants.ONE){
                    return JSONMessage.failure("接口URL解析域名失败");
                }
                if (domain.size() < KConstants.ONE || Collections.disjoint(domain, domain_)){
                    return JSONMessage.failure("APP下载地址与入口地址的主域名不一致");
                }
            }

            clientConfig.setInterfaceOrder(new ClientConfig.InterfaceOrder(clientConfig));

            adminManager.setClientConfig(clientConfig);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }


    /**
     * 获取客户端配置
     **/
    @RequestMapping(value = "/clientConfig")
    public JSONMessage getClientConfig() {
        ClientConfig clientConfig = adminManager.getClientConfig();
        return JSONMessage.success(null, clientConfig);
    }


    /**
     * 设置支付配置
     **/
    @RequestMapping(value = "/payConfig/set")
    public JSONMessage setPayConfig(@ModelAttribute PayConfig payConfig){
        try {
            if(payConfig.getMyChangeWithdrawRate() < 0.006){
                return JSONMessage.failure("费率不能低于0.006");
            }
            adminManager.setPayConfig(payConfig);
            return JSONMessage.success();
        }catch (Exception e){
            return JSONMessage.failure(e.getMessage());
        }
    }


    /**
     * 获取支付配置
     **/
    @RequestMapping(value = "/payConfig")
    public JSONMessage getPayConfig(){
        PayConfig payConfig = adminManager.getPayConfig();
        return JSONMessage.success(payConfig);
    }


    /**
     * 保存总配置
     *
     * @param totalConfig
     * @return
     */
    @RequestMapping(value = "/addTotalConfig")
    public JSONMessage addTotalConfig(@ModelAttribute TotalConfig totalConfig) {
        try {
            adminManager.addTotalConfig(totalConfig);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }


    /**
     * 获取 短信 配置项
     **/
    @ApiOperation("获取 短信 配置项")
    @RequestMapping(value = "/smsConfig")
    public JSONMessage getSmsConfig() {
        return JSONMessage.success(null, SKBeanUtils.getImCoreService().getSmsConfig());
    }


    /**
     * 设置 短信 配置项
     **/
    @ApiOperation("设置 短信 配置项")
    @RequestMapping(value = "/sms/config/set", method = RequestMethod.POST)
    public JSONMessage setSmsConfig(@ModelAttribute SmsConfig smsConfig) {
        try {
            adminManager.setSmsConfig(smsConfig);
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failure(e.getMessage());
        }
    }

    @ApiOperation("设置全局短信配置")
    @RequestMapping(value = "/sms/application/config/set", method = RequestMethod.POST)
    public JSONMessage setAppliactionSmsConfig(@ModelAttribute SmsConfig smsConfig){
        adminManager.setAppliactionSmsConfig(smsConfig);
        return JSONMessage.success();
    }

    @ApiOperation("设置阿里云短信配置")
    @RequestMapping(value = "/sms/aliyun/config/set", method = RequestMethod.POST)
    public JSONMessage setAliyunSmsConfig(@ModelAttribute SmsConfig smsConfig){
        adminManager.setAliyunSmsConfig(smsConfig);
        return JSONMessage.success();
    }

    @ApiOperation("设置天天国际短信配置")
    @RequestMapping(value = "/sms/tistilo/config/set", method = RequestMethod.POST)
    public JSONMessage setTistiloSmsConfig(@ModelAttribute SmsConfig smsConfig){
        adminManager.setTistiloSmsConfig(smsConfig);
        return JSONMessage.success();
    }

    /**
     * 设置全站公告信息
     * status 0=不展示 1=永久展示 2= 只展示一次
     * type 0=文字 1=图片
     * slideshow 一直走马灯展示 0=关 1=开
     * content 文本内容
     * picturn 图片
     **/
    @ApiOperation("设置全站公告信息")
    @RequestMapping(value = "/notice/config/set")
    public JSONMessage setNoticeConfig(@RequestParam(defaultValue = "")String status,@RequestParam(defaultValue = "")String type,
                                       @RequestParam(defaultValue = "")String slideshow,@RequestParam(defaultValue = "")String content,
                                       @RequestParam(defaultValue = "") String picturn) {
        Map<String,Object> mapData = new HashMap<>();
        mapData.put("id",new ObjectId().toString());
        mapData.put("status",status);
        mapData.put("type",type);
        mapData.put("slideshow",slideshow);
        mapData.put("content",content);
        mapData.put("picturn",picturn);
        JSONObject jsonObject = new JSONObject(mapData);
        adminRedisRepository.saveNoticeConfig(jsonObject.toJSONString());
        return JSONMessage.success();
    }

    /**
     * 查询全站公告信息
     * @return
     */
    @ApiOperation("查询全站公告信息")
    @RequestMapping(value = "/find/notice/config")
    public JSONMessage findNoticeConfig(){
        String noticeConfig = adminRedisRepository.getNoticeConfig();
        JSONObject jsonObject = JSON.parseObject(noticeConfig);
        return JSONMessage.success(jsonObject);
    }

    /**
     * 设置热门主页
     * @return
     */
    @ApiOperation("设置热门主页")
    @RequestMapping(value = "/homepage/config/set")
    public JSONMessage setNoticeConfig(@RequestParam(defaultValue = "")String name,@RequestParam(defaultValue = "")String homeUrl,
                                       @RequestParam(defaultValue = "")String imgUrl) {
        Map<String,Object> mapData = new HashMap<>();
        mapData.put("name",name);
        mapData.put("homeUrl",homeUrl);
        mapData.put("imgUrl",imgUrl);
        JSONObject jsonObject = new JSONObject(mapData);
        ClientConfig clientConfig = SKBeanUtils.getImCoreService().getClientConfig();
        clientConfig.setHomeAddress(jsonObject.toJSONString());
        SKBeanUtils.getImCoreService().setClientConfig(clientConfig);
        return JSONMessage.success();
    }


    @ApiOperation("设置签到")
    @RequestMapping(value = "/sign/config/set")
    public JSONMessage setSign(@RequestParam(defaultValue = "") String signStr) {
        String[] data = signStr.split(",");
        try {
            Arrays.stream(data).forEach(val->{ MoneyUtil.fromYuanToCent(val.split(":")[1]);});
        }catch (Exception e){
            return JSONMessage.failure("设置的金额错误");
        }

        adminRedisRepository.setSignMaxCount(Long.valueOf(data.length));
        for (int i = 0; i < data.length; i++) {
            String[] split = data[i].split(":");
            adminRedisRepository.setSignPolicyAward(Long.valueOf(split[0]), (int) MoneyUtil.fromYuanToCent(split[1]));
        }
        return JSONMessage.success();
    }

    @ApiOperation("获取签到")
    @RequestMapping(value = "/sign/config/get")
    public JSONMessage getSign() {
        Map<Long, Integer> signPolicyAwardMap = adminRedisRepository.getSignPolicyAward();
        ObjectMapper mapper = new ObjectMapper();
        try {
            String writeValueAsString = mapper.writeValueAsString(signPolicyAwardMap);
            return JSONMessage.success(writeValueAsString);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }


    @ApiOperation("验证密码是否正确")
    @RequestMapping(value = "/verify/password")
    public JSONMessage verifyPassword(String password, String account, String areaCode) {
        User user = userManager.getUser((com.basic.im.comm.utils.StringUtil.isEmpty(areaCode) ? (areaCode + account) : (areaCode + account)));
        if (ObjectUtil.isNull(user) ) {
            return JSONMessage.failure("账号不存在");
        }

        if (!password.equals(user.getPassword())) {
            String  dbpassword = authKeysService.queryLoginPassword(user.getUserId());
            password = LoginPassword.encodeFromOldPassword(password);
            if (!password.equals(dbpassword)) {
                return JSONMessage.failureByErrCodeAndData(2,"帐号或密码错误");
            }
        }
        return JSONMessage.success();
    }



}
