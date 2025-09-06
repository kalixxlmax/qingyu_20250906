package com.basic.im.api.controller;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.admin.dao.KeywordDAO;
import com.basic.im.api.AbstractController;
import com.basic.im.api.IpSearch;
import com.basic.im.api.service.AuthServiceUtils;
import com.basic.im.api.utils.NetworkUtil;
import com.basic.im.appleLogin.SignInWithApple;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.RandomUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.common.MultipointSyncUtil;
import com.basic.im.oneclickLogin.OneCickLoginApi;
import com.basic.im.sms.service.SMSServiceImpl;
import com.basic.im.user.dao.SdkLoginInfoDao;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.AuthKeys;
import com.basic.im.user.entity.User;
import com.basic.im.user.model.*;
import com.basic.im.user.service.AllowRequestClientService;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.user.service.impl.AuthKeysServiceImpl;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.Base64;
import com.basic.utils.StringUtil;
import com.basic.utils.encrypt.AES;
import com.basic.utils.encrypt.MD5;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * UserController  中方法太多了比较乱
 * 将 用户 账号 登陆 注册 第三方 账号 相关操作 抽取处理
 */

@RestController
@Api(value="AccountController",tags="用户账号登陆注册相关操作  新接口")
@RequestMapping(value="",method={RequestMethod.GET,RequestMethod.POST})
public class AccountController extends AbstractController {


    @Autowired
    private AuthKeysServiceImpl authKeysService;
    @Autowired
    private UserDao userDao;

    @Autowired
    private SMSServiceImpl smsService;
    
    @Autowired
    private UserRedisService userRedisService;
    
    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private UserCoreService userCoreService;

    @Autowired
    private AuthServiceUtils authServiceUtils;

    @Autowired
    private SdkLoginInfoDao sdkLoginInfoDao;

    @Autowired
    private UserCoreRedisRepository userCoreRedisRepository;
    @Autowired
    private OneCickLoginApi oneCickLoginApi;

    @Autowired
    private KeywordDAO keywordDAO;


    @ApiOperation(value = "用户注册V1 新接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="deviceId" , value="设备类型 android ios",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true),
    })
    @RequestMapping(value = "/user/register/v1")
    public JSONMessage registerV1(@RequestParam(defaultValue = "") String deviceId,
                                  @RequestParam String data,
                                  @RequestParam String salt,
                                  HttpServletRequest request) {
        try {

            //关闭注册
            if( 0 == SKBeanUtils.getImCoreService().getClientConfig().getIsOpenRegister() ){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NoRegistrationFunctionIsProvided);
            }


            JSONObject jsonObject = authServiceUtils.authApiKeyCheckSign(data, salt);
            if(null==jsonObject){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            UserExample example = userManager.authRegedit(jsonObject, salt);
            example.setAreaCode("86");
//            if(!"86".equals(example.getAreaCode())){
//                return JSONMessage.failure("请填写中国手机号!");
//            }
            if (StringUtil.isEmpty(example.getTelephone())) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.PleaseFallTelephone);
            } else if (userManager.isRegister(example.getAreaCode() + example.getTelephone())) {
                if (1 == SKBeanUtils.getImCoreService().getConfig().getRegeditPhoneOrName())//用戶名註冊
                {
                    return JSONMessage.failureByErrCode(KConstants.ResultCode.AccountExist);
                }
                return JSONMessage.failureByErrCode(KConstants.ResultCode.TelephoneIsRegister);
            } else if (StringUtil.isEmpty(example.getNickname())) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.PleaseNickName);
            } else if (userManager.checkUserNameNotwords(example.getNickname())) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NicknameWrongful);
            }

             if (keywordDAO.queryByWord(example.getNickname())){
                 return JSONMessage.failureByErrCode(KConstants.ResultCode.NicknameWrongful);
             }
            KeyPairParam param = jsonObject.toJavaObject(KeyPairParam.class);
            example.setDeviceType(getDeviceType(request.getHeader("User-Agent")));
            example.setDeviceId(deviceId);
            example.setPhone(example.getTelephone());
            example.setTelephone(example.getAreaCode() + example.getTelephone());
            Map<String, Object> result = userManager.registerIMUser(example);
            authKeysService.uploadMsgKey(example.getUserId(), param);
            authKeysService.updateLoginPassword(example.getUserId(), example.getPassword());
            String jsonRsult = JSONObject.toJSONString(result);
            jsonRsult = AES.encryptBase64(jsonRsult, MD5.encrypt(authServiceUtils.getApiKey()));
            Map<String, Object> dataMap = new HashMap<>(1);
            dataMap.put("data", jsonRsult);
            return JSONMessage.success(dataMap);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation(value = "用户微信注册V1 新接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true)
    })
    @RequestMapping(value = "/user/registerSDK/v1")
    public JSONMessage registerSDKV1(@RequestParam String data, @RequestParam String salt) {
        try {

            //关闭注册
            if( 0 == SKBeanUtils.getImCoreService().getClientConfig().getIsOpenRegister() ){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.NoRegistrationFunctionIsProvided);
            }


            JSONObject jsonObject = authServiceUtils.authApiKeyCheckSign(data, salt);
            if(null==jsonObject){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            UserExample example = userManager.authRegedit(jsonObject, salt);
            example.setAreaCode("86");
            KeyPairParam param=jsonObject.toJavaObject(KeyPairParam.class);
            example.setPhone(example.getTelephone());
            example.setTelephone(example.getAreaCode() + example.getTelephone());
            example.setAccount(jsonObject.getString("loginInfo"));
            example.setLoginType(jsonObject.getIntValue("type"));
            if(0==example.getLoginType()){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
            }
            Map<String,Object> result= userManager.registerIMUserBySdk(example, example.getLoginType());
            authKeysService.uploadMsgKey(example.getUserId(),param);
            authKeysService.updateLoginPassword(example.getUserId(),example.getPassword());
            String jsonRsult= JSONObject.toJSONString(result);
            jsonRsult= AES.encryptBase64(jsonRsult, MD5.encrypt(authServiceUtils.getApiKey()));
            Map<String, Object> dataMap=new HashMap<>(1);
            dataMap.put("data",jsonRsult);
            return JSONMessage.success(dataMap);
        } catch (Exception e) {
           return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation(value = "用户微信绑定手机号")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true)
    })
    @RequestMapping(value = "/user/bindingTelephone/v1")
    public JSONMessage bindingTelephoneV1(HttpServletRequest request, @ModelAttribute LoginExample example, @RequestParam(defaultValue ="") String data, @RequestParam(defaultValue ="") String salt){
        try {
            example.setAreaCode("86");
            example.setDeviceType(getDeviceType(request.getHeader("User-Agent")));
            User user=userManager.getUser(example.getUserId());
            // 账号不存在
            if(null==user) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.SdkLoginNotExist);
            }
            String code = userRedisService.queryLoginSignCode(user.getUserId(), example.getDeviceId());

            if(StringUtil.isEmpty(code)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            userRedisService.cleanLoginCode(example.getUserId(),example.getDeviceId());
            byte[] deCode= Base64.decode(code);

            JSONObject jsonParam = authServiceUtils.authUserLoginCheck(example.getUserId(), data, salt, user.getPassword(), deCode);
            if(null==jsonParam) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            LoginExample jsonExample=jsonParam.toJavaObject(LoginExample.class);
            if(null==jsonExample) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            jsonExample.copySignExample(example);
            jsonExample.setLoginType(jsonParam.getIntValue("type"));
            if(0==jsonExample.getLoginType()){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
            }
            // 单个账号绑定多个第三方账号的情况
            if(null != sdkLoginInfoDao.getSdkLoginInfo(jsonExample.getLoginType(),example.getUserId())){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.repeatThirdPartyAlreadyBound);
            }
            userManager.addSdkLoginInfo(jsonExample.getLoginType(), user.getUserId(), jsonParam.getString("loginInfo"));
            jsonExample.setIsSdkLogin(1);
            Map<String,Object> result = userManager.loginV1(jsonExample);
            String jsonRsult=JSONObject.toJSONString(result);
            jsonRsult=AES.encryptBase64(jsonRsult,deCode);
            Map<String, Object> dataMap=new HashMap<>(1);
            dataMap.put("data",jsonRsult);
            return JSONMessage.success(null, dataMap);
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

    }

    @ApiOperation(value = "用户绑定手机号 已登录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="telephone" , value="电话号码 加区号",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="areaCode" , value="区号",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="code" , value="短信验证码",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="password" , value="密码 换手机号不需要密码",dataType="String",required=false)
    })
    @RequestMapping(value = "/user/bindingTelephone/v2")
    public JSONMessage bindingTelephoneV2(@RequestParam(defaultValue = "") String telephone,@RequestParam(defaultValue = "") String areaCode,@RequestParam(defaultValue = "") String code,@RequestParam(defaultValue = "") String password,@ModelAttribute KeyPairParam param){
        try {
            areaCode="86";
            Integer userId = ReqUtil.getUserId();
            User oldUser=userDao.getUser(telephone);
            // 账号不存在
            if(null!=oldUser) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.TelephoneIsRegister);
            }

            //判断短信验证码
            if(!smsService.isAvailable(telephone,code)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.VerifyCodeErrOrExpired);
            }
            //查出用户
            User user=userManager.getUser(userId);//根据用户编号搜索用户
            if(null==user) {//判断用户是否存在
                return JSONMessage.failureByErrCode(KConstants.ResultCode.UserNotExist);
            }

            if (StringUtil.isEmpty(telephone) || StringUtil.isEmpty(code)|| StringUtil.isEmpty(areaCode)){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);
            }
            /**
             * 第一次绑定必须设置密码
             */
            if(StringUtil.isEmpty(user.getTelephone())&&StringUtil.isEmpty(password)){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);
            }else if(StringUtil.isEmpty(user.getTelephone())) {
                authKeysService.updateLoginPassword(userId,password);
                //更新用户公钥私钥
                authKeysService.uploadMsgKey(userId, param);

            }
            //保存手机
            userDao.bindingTelephone(userId,telephone,areaCode,password);
            userRedisService.deleteUserByAccount(user.getAccount());
            userCoreRedisRepository.deleteUserByUserId(userId);


            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

    }



    @ApiOperation(value = "绑定微信账号 新接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="loginInfo" , value="微信openId",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true)
    })
    @RequestMapping(value = "/user/bindWxAccount")
    public JSONMessage bindWxAccount(@ModelAttribute LoginExample example,@RequestParam(defaultValue ="") String data, @RequestParam(defaultValue ="") String salt
            ,@RequestParam(defaultValue ="") String loginInfo,@RequestParam(defaultValue ="2") int type){
        try {
            example.setAreaCode("86");
            Integer userId = ReqUtil.getUserId();
            User user=userManager.getUser(userId);
            // 账号不存在
            if(null==user) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.SdkLoginNotExist);
            }
            if(0==type){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
            }
            example.setUserId(userId);
            //String code = userRedisService.queryLoginSignCode(user.getUserId(), example.getDeviceId());

            SdkLoginInfo sdkLoginInfo = userManager.findSdkLoginInfo(type,loginInfo);
            if (null!=sdkLoginInfo&&null!=sdkLoginInfo.getUserId()){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.ThirdPartyAlreadyBound);
            }
            sdkLoginInfoDao.initLoginInfoUserId(type,loginInfo,userId);

            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

    }
    @ApiOperation(value = "用户第三方信息登录（微信，QQ,一键登录，Apple登录")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="loginInfo" , value="微信openId",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true)
    })
    @RequestMapping(value = "/user/sdkLogin/v1")
    public JSONMessage sdkLoginV1(HttpServletRequest request,@ModelAttribute LoginExample example,@RequestParam String data,@RequestParam String salt) {
        example.setAreaCode("86");
        example.setDeviceType(getDeviceType(request.getHeader("User-Agent")));
        JSONObject jsonParam = authServiceUtils.decodeApiKeyDataJson(data);
        jsonParam = authServiceUtils.authWxLoginCheck(jsonParam, data, salt);
        if (null == jsonParam) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
        }
        LoginExample jsonExample = jsonParam.toJavaObject(LoginExample.class);
        if (null == jsonExample) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
        }
        jsonExample.copySignExample(example);
        jsonExample.setAccount(jsonParam.getString("loginInfo"));
        jsonExample.setLoginType(jsonParam.getIntValue("type"));
        if (0 == jsonExample.getLoginType()) {
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
        //手机号一键登录
        if (3 == jsonExample.getLoginType()) {
            String formatMobile = oneCickLoginApi.getFormatMobile(jsonExample.getAccount());
            jsonExample.setAccount(formatMobile);
        }
        // apple登录
        if(4 == jsonExample.getLoginType()) {
            try{
                String appleLoginCode = SignInWithApple.appleLogin(jsonExample.getAccount(),jsonParam.getString("appleLoginCode"));
                jsonExample.setAccount(appleLoginCode);
            } catch (Exception e) {
                return JSONMessage.failureByException(e);
            }
        }
        Map<String, Object> result = null;
        UserExample userExample=null;
        SdkLoginInfo sdkLoginInfo = sdkLoginInfoDao.querySdkLoginInfo(jsonExample.getAccount());
        //获取请求ip地址
        String ip= NetworkUtil.getIpAddress(request);
        //获取语言
        String area= IpSearch.getArea(ip);
        logger.info("sdkLogin type {} account {} ip {} area {}",jsonExample.getLoginType(),jsonExample.getAccount(),ip,area);
        // 未绑定手机号码
        if (null != sdkLoginInfo) {

            try {
                if (null == sdkLoginInfo.getUserId() || 0 == sdkLoginInfo.getUserId()) {
                    if (3 != sdkLoginInfo.getType() && 0 == SKBeanUtils.getImCoreService().getClientConfig().getIsNoRegisterThirdLogin()) {
                        if(4 == sdkLoginInfo.getType()&&1==SKBeanUtils.getImCoreService().getClientConfig().getUpperShelf()) {
                            /**
                             * 上架模式
                             */
                        }else {
                            return JSONMessage.failureByErrCode(KConstants.ResultCode.UNBindingTelephone);
                        }/*else if(4 == sdkLoginInfo.getType()&&1!=SKBeanUtils.getImCoreService().getClientConfig().getUpperShelf()){
                            if(!StringUtil.isEmpty(area)&&"CN".equals(area)) {
                                return JSONMessage.failureByErrCode(KConstants.ResultCode.UNBindingTelephone);
                            }
                        }else {
                            return JSONMessage.failureByErrCode(KConstants.ResultCode.UNBindingTelephone);
                        }*/
                    }
                    userExample = new UserExample();
                    userExample.setAccount(jsonExample.getAccount());//账号 -- openid
                    //获取微信用户信息
                    String userData = sdkLoginInfo.getUserData();
                    if (StringUtil.isEmpty(userData)) {
                        return JSONMessage.failureByErrCode(KConstants.ResultCode.GetOpenIdFailure);
                    }
                    JSONObject wxUserInfo = JSONObject.parseObject(userData);
                    logger.info("这是用户数据 :{} ", wxUserInfo);
                    if (wxUserInfo.getString("openid") == null) {
                        System.out.println(" 用户数据为空！ 展示虚拟数据 ");
                        wxUserInfo = JSONObject.parseObject("{\"openid\":" + "\" " + jsonExample.getAccount() + "\",\"nickname\": " + "\" " + "您好" + " \",\"sex\":1,\"language\":\"zh_CN\",\"city\":\"\",\"province\":\"Moscow\",\"country\":\"RU\",\"headimgurl\":\"http:\\/\\/thirdwx.qlogo.cn\\/mmopen\\/vi_32\\/DYAIOgq83er0mAxKX6eKqDEM88bGBR6e0tSBrxfRO2sp7kZlz5XT7WqCJNoh2HfqJvHlKh9q8OZKEUPCatcXBA\\/132\",\"privilege\":[],\"unionid\":\"ovDOz1X3r9GUnWt9-mZTH9QR3Kc8\"}");
                    }
                    //设置用户昵称
                    String nickname = wxUserInfo.getString("nickname");
                    //解决乱码问题
                    try {
                        nickname = new String(nickname.getBytes("ISO-8859-1"), "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    userExample.setNickname(nickname);
                    //设置用户性别
                    String sex = wxUserInfo.getString("sex");
                    if (sex.equals("1")) {
                        userExample.setSex(0);
                    } else {
                        userExample.setSex(1);
                    }
                    //设置城市名称
                    userExample.setCityName(wxUserInfo.getString("city"));
                    //设置微信头像
                    userExample.setHeadimgurl(wxUserInfo.getString("headimgurl"));
                    userExample.setUserType(0);
                    result = userManager.registerIMUser(userExample);
                    sdkLoginInfoDao.initLoginInfoUserId(sdkLoginInfo.getType(), sdkLoginInfo.getLoginInfo(), Integer.valueOf(result.get("userId").toString()));
                    User user = userManager.getUser(userExample.getUserId());
                    example.setUserId(userExample.getUserId());
                    jsonExample.setUserId(user.getUserId());
                    result = userManager.loginSuccessV1(user, example);
                    result.put("headimgurl", userExample.getHeadimgurl());
                } else {
                    if (null != sdkLoginInfo.getUserId()) {
                        jsonExample.setUserId(sdkLoginInfo.getUserId());//设置用户编号
                    }
                    User user = userManager.getUser(sdkLoginInfo.getUserId());
                    if (null == user) {
                        sdkLoginInfoDao.deleteSdkLoginInfo(sdkLoginInfo.getType(), sdkLoginInfo.getUserId());
                        return JSONMessage.failureByErrCode(KConstants.ResultCode.UserNotExist);
                    }
                    if (3 != sdkLoginInfo.getType() && 0 == SKBeanUtils.getImCoreService().getClientConfig().getIsNoRegisterThirdLogin() && StringUtil.isEmpty(user.getTelephone())) {
                        if(4 == sdkLoginInfo.getType()&&1==SKBeanUtils.getImCoreService().getClientConfig().getUpperShelf()) {
                            /**
                             * 上架模式
                             */
                        }else {
                           return JSONMessage.failureByErrCode(KConstants.ResultCode.UNBindingTelephone);
                        }
                    }
                    jsonExample.setUserId(user.getUserId());
                    result = userManager.loginV1(jsonExample);
                }

            } catch (Exception e) {
                return JSONMessage.failureByException(e);
            }
        } else {
            // 因App Store要求苹果登录不允许要求绑定其他业务账号，所以这里不用处理苹果登录绑定到IM账号
            if(4 == jsonExample.getLoginType()&&1==SKBeanUtils.getImCoreService().getClientConfig().getUpperShelf()) {

                /**
                 * 上架模式
                 */
            }else {
                if(0 == SKBeanUtils.getImCoreService().getClientConfig().getIsNoRegisterThirdLogin()) {
                    if (3 == jsonExample.getLoginType()) {

                    } else {
                        return JSONMessage.failureByErrCode(KConstants.ResultCode.UNBindingTelephone);
                    }
                }
            }
            /**
             * QQ 登陆
             */
            if(1 == jsonExample.getLoginType()) {
                 userExample = new UserExample();
                userExample.setAccount(jsonExample.getAccount());//账号 -- openid
                userExample.setNickname(jsonParam.getString("nickName"));
                userExample.setHeadimgurl(jsonParam.getString("headImageUrl"));
                result = userManager.registerIMUser(userExample);
                sdkLoginInfo = new SdkLoginInfo();
                sdkLoginInfo.setLoginInfo(jsonExample.getAccount());
                sdkLoginInfo.setType(jsonExample.getLoginType());
                sdkLoginInfo.setUserId(Integer.valueOf(result.get("userId").toString()));
                sdkLoginInfoDao.addSdkLoginInfo(sdkLoginInfo.getType(), sdkLoginInfo.getUserId(), sdkLoginInfo.getLoginInfo());
                User user = userManager.getUser(userExample.getUserId());
                example.setUserId(userExample.getUserId());
                jsonExample.setUserId(user.getUserId());
                result = userManager.loginSuccessV1(user, example);
                result.put("headimgurl", userExample.getHeadimgurl());
            }else if (3 == jsonExample.getLoginType()) {
                // 一键登录
                String phone = jsonExample.getAccount();
                String telephone = "86" + phone;
                User user = null;
                try {
                    user = userManager.getUser(telephone);
                } catch (Exception e) {
                    logger.info("未注册过得账号一键登录");
                }
                sdkLoginInfo = new SdkLoginInfo();
                if (null != user) {
                    sdkLoginInfo.setUserId(user.getUserId());
                } else {
                    return JSONMessage.failureByErrCode(KConstants.ResultCode.UNBindingTelephone);
                    /*userExample = new UserExample();
                    userExample.setAccount(phone);//账号 -- openid
                    userExample.setNickname(RandomUtil.getRandomEnAndNum(6));
                    userExample.setTelephone(telephone);
                    userExample.setPhone(phone);
                    try {
                        result = userManager.registerIMUser(userExample);
                        user = userManager.getUser(userExample.getUserId());
                        if(null!=request) {
                            sdkLoginInfo.setUserId(Integer.valueOf(result.get("userId").toString()));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }*/

                }

                jsonExample.setUserId(user.getUserId());
                example.setUserId(user.getUserId());

                sdkLoginInfo.setLoginInfo(jsonExample.getAccount());
                sdkLoginInfo.setType(jsonExample.getLoginType());
                sdkLoginInfoDao.addSdkLoginInfo(sdkLoginInfo.getType(), sdkLoginInfo.getUserId(), sdkLoginInfo.getLoginInfo());
                result = userManager.loginSuccessV1(user, example);
            } else if (4 == jsonExample.getLoginType()) {
                // Apple登录
                 userExample = new UserExample();
                userExample.setAccount(jsonExample.getAccount());//账号 -- openid
                userExample.setNickname(RandomUtil.getRandomEnAndNum(6));
                result = userManager.registerIMUser(userExample);
                sdkLoginInfo = new SdkLoginInfo();
                sdkLoginInfo.setLoginInfo(jsonExample.getAccount());
                sdkLoginInfo.setType(jsonExample.getLoginType());
                sdkLoginInfo.setUserId(Integer.valueOf(result.get("userId").toString()));
                sdkLoginInfoDao.addSdkLoginInfo(sdkLoginInfo.getType(), sdkLoginInfo.getUserId(), sdkLoginInfo.getLoginInfo());
                User appleUser = userManager.getUser(userExample.getUserId());
                example.setUserId(userExample.getUserId());
                jsonExample.setUserId(appleUser.getUserId());
                result = userManager.loginSuccessV1(appleUser, example);

            } else {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.GetOpenIdFailure);
            }
        }
        byte isOpenSecureChat = SKBeanUtils.getImCoreService().getClientConfig().getIsOpenSecureChat();
        if(1 == isOpenSecureChat){
            //result.put("isSupportSecureChat", 1);
            AuthKeys authKeys = authKeysService.getAuthKeys(jsonExample.getUserId());
            logger.info("userId {} authKeys {} ", JSON.toJSONString(authKeys));
            if (null != authKeys && null != authKeys.getMsgDHKeyPair() && !StringUtil.isEmpty(authKeys.getMsgDHKeyPair().getPrivateKey())) {
                result.put("isSupportSecureChat", 1);
            }
        }
        String jsonRsult = JSONObject.toJSONString(result);
        jsonRsult = AES.encryptBase64(jsonRsult, MD5.encrypt(authServiceUtils.getApiKey()));
        Map<String, Object> dataMap = new HashMap<>(1);
        dataMap.put("data", jsonRsult);
        return JSONMessage.success(dataMap);
    }

    @ApiOperation(value = "用户短信登陆 新接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true)
    })
    @RequestMapping(value = "/user/smsLogin")
    public JSONMessage smsLogin(HttpServletRequest request,@ModelAttribute LoginExample example,@RequestParam(defaultValue ="") String data,@RequestParam(defaultValue ="") String salt) {
        try {
			/*if(null == example.getVerificationCode())
				return JSONMessage.failureByErrCode(KConstants.ResultCode.SMSCanNotEmpty);*/
            example.setAreaCode("86");
            example.setDeviceType(getDeviceType(request.getHeader("User-Agent")));
            example.setTelephone(example.getAreaCode()+example.getAccount());
            String smsCode = smsService.getSmsCode(example.getTelephone());
            if(StringUtil.isEmpty(smsCode)){
                return JSONMessage.failureByErrCode(KConstants.ResultCode.VerifyCodeErrOrExpired);
            }
            byte[] decode = MD5.encrypt(smsCode);
            JSONObject jsonParam = authServiceUtils.authSmsLoginCheck(example.getTelephone(),decode,data,salt);
            if(null==jsonParam) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.VerifyCodeErrOrExpired);
            }
            LoginExample jsonExample=jsonParam.toJavaObject(LoginExample.class);
            if(null==jsonExample) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            jsonExample.copySignExample(example);

            Map<String,Object> result = userManager.smsLogin(jsonExample);
            String jsonRsult=JSONObject.toJSONString(result);
            jsonRsult=AES.encryptBase64(jsonRsult,decode);
            Map<String, Object> dataMap=new HashMap<>(1);
            dataMap.put("data",jsonRsult);
            return JSONMessage.success(null, dataMap);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }


    @Autowired(required = false)
    private AllowRequestClientService allowRequestClientService;

    @ApiOperation(value = "用户密码登陆 新接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true)
    })
    @RequestMapping(value = "/user/login/v1")
    public JSONMessage loginV1(HttpServletRequest request,
                               @ModelAttribute LoginExample example,
                               @RequestParam(defaultValue ="") String data,
                               @RequestParam(defaultValue ="") String salt) {
        try {

            example.setAreaCode("86");
            example.setDeviceType(getDeviceType(request.getHeader("User-Agent")));
            User user = userManager.getUser(example.getUserId());
            String code = userRedisService.queryLoginSignCode(user.getUserId(), example.getDeviceId());

            if(StringUtil.isEmpty(code)) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            userRedisService.cleanLoginCode(example.getUserId(),example.getDeviceId());
            byte[] deCode= Base64.decode(code);
            String password = authKeysService.queryLoginPassword(example.getUserId());
            JSONObject jsonParam =authServiceUtils.authUserLoginCheck(example.getUserId(),data,salt,password,deCode);
            if(null==jsonParam) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            LoginExample jsonExample = jsonParam.toJavaObject(LoginExample.class);
            if(null==jsonExample) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }

            jsonExample.copySignExample(example);
            /**
             *1、只有授权的IPオ能登录使用PC端ー一防止离职员工或同行,异地使用PC端
             * 2、只有授权的IP才能登录使用网页端一一防止离职员工或同行,异地使用网页端;
             *
             */
            if(StrUtil.isEmpty(example.getDeviceKey())){
                example.setDeviceKey(example.getDeviceId());
            }
            if(!StrUtil.isEmpty(example.getDeviceKey())&&
                    ("web".equals(example.getDeviceKey())|| "pc".equals(example.getDeviceKey()))){
                if(!allowRequestClientService.isAllowRequest(getRequestIp())){
                    return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
                }
            }
            jsonExample.setLanguage(ReqUtil.getRequestLanguage());
            jsonExample.setIpAddress(NetworkUtil.getIpAddress(request));
            Map<String, Object> result = userManager.loginV1(jsonExample);
            String jsonRsult=JSONObject.toJSONString(result);
            jsonRsult=AES.encryptBase64(jsonRsult,deCode);
            Map<String, Object> dataMap=new HashMap<>(1);
            dataMap.put("data",jsonRsult);
            return JSONMessage.success(null, dataMap);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }

    @ApiOperation(value = "用户自动登陆 新接口")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="data" , value="加密数据",dataType="String",required=true),
            @ApiImplicitParam(paramType="query" , name="salt" , value="盐值",dataType="String",required=true)
    })
    @RequestMapping(value = "/user/login/auto/v1")
    public JSONMessage loginAutoV1(HttpServletRequest request,@ModelAttribute LoginExample example,@RequestParam(defaultValue ="") String data,
                                   @RequestParam(defaultValue ="") String loginToken,@RequestParam(defaultValue ="") String salt) {
        try {
            example.setAreaCode("86");
            // 兼容旧包
            if(0 != example.getUserId()){
                example.setDeviceType(getDeviceType(request.getHeader("User-Agent")).contains("Android")? "android" : "ios");
                // loginToken是否过期
                String loginTokenDevice = userRedisService.queryLoginToken(example.getUserId(), example.getDeviceType());
                if(StringUtil.isEmpty(loginTokenDevice)){
                    return JSONMessage.failureByErrCode(KConstants.ResultCode.LoginTokenExpired);
                }
            }
            UserLoginTokenKey loginTokenKey = userRedisService.queryLoginTokenKeys(loginToken);
            if(null==loginTokenKey){
                // 新设备登录导致loginToken过期
                return JSONMessage.failureByErrCode(KConstants.ResultCode.LoginTokenInvalid);
            }
            example.setUserId(loginTokenKey.getUserId());
            example.setLanguage(ReqUtil.getRequestLanguage());
            example.setDeviceId(loginTokenKey.getDeviceId());
            JSONObject jsonParam = authServiceUtils.authUserAutoLoginCheck(example.getUserId(), loginToken, loginTokenKey.getLoginKey(), salt, data);
            if(null==jsonParam) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            LoginExample jsonExample=jsonParam.toJavaObject(LoginExample.class);
            if(null==jsonExample) {
                return JSONMessage.failureByErrCode(KConstants.ResultCode.AUTH_FAILED);
            }
            jsonExample.copySignExample(example);
            /**
             *1、只有授权的IPオ能登录使用PC端ー一防止离职员工或同行,异地使用PC端
             * 2、只有授权的IP才能登录使用网页端一一防止离职员工或同行,异地使用网页端;
             *
             */
            if(StrUtil.isEmpty(example.getDeviceKey())){
                example.setDeviceKey(example.getDeviceId());
            }
            if(!StrUtil.isEmpty(example.getDeviceKey())&&
                    ("web".equals(example.getDeviceKey())|| "pc".equals(example.getDeviceKey()))){
                if(!allowRequestClientService.isAllowRequest(getRequestIp())){
                    return JSONMessage.failureByErrCode(KConstants.ResultCode.NO_PERMISSION);
                }
            }
            jsonExample.setIpAddress(NetworkUtil.getIpAddress(request));
            Object result = userManager.loginAutoV1(jsonExample,loginTokenKey,null);
            String jsonRsult=JSONObject.toJSONString(result);
            jsonRsult=AES.encryptBase64(jsonRsult,Base64.decode(loginTokenKey.getLoginKey()));
            Map<String, Object> dataMap=new HashMap<>(1);
            dataMap.put("data",jsonRsult);
            return JSONMessage.success(null, dataMap);
        } catch (ServiceException e) {
            return JSONMessage.failureByException(e);
        }
    }




    @ApiOperation("查询用户是否设置密码")
    @RequestMapping(value = "/user/isExistPassword")
    public JSONMessage getUserLoginPassword(){
        Integer userId = ReqUtil.getUserId();
        AuthKeys authKeys = authKeysService.getAuthKeys(userId);
        if(null!=authKeys&&!StrUtil.isEmpty(authKeys.getPassword())){
            return JSONMessage.success();
        }else {
            return JSONMessage.failure("没有设置登陆密码!");
        }


    }

    /**
     * 解除绑定
     * @param type
     * @return
     */
    @ApiOperation("解除绑定微信")
    @ApiImplicitParam(paramType="query" , name="type",value="类型",dataType="int")
    @RequestMapping(value = "/user/unbind")
    public JSONMessage unbind(@RequestParam(defaultValue="2") int type){
        if(0==type){
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
        }
         userManager.unbind(type, ReqUtil.getUserId());
        return JSONMessage.success();
    }

    /**
     * 获取用户绑定信息
     * @return
     */

    @ApiOperation("获取用户微信绑定信息")
    @RequestMapping(value="/user/getBindInfo")
    public JSONMessage getBingInfo(){
        Object data = userManager.getBindInfo(ReqUtil.getUserId());
        return JSONMessage.success(data);
    }

    /**
     * 获取微信 openid
     * @param code
     * @return
     */
    @ApiOperation("获取微信 openid")
    @ApiImplicitParam(paramType="query" , name="code",value="标识码",dataType="String")
    @RequestMapping(value ="/user/getWxOpenId")
    public JSONMessage getWxOpenId(@RequestParam String code){
        Object data=userManager.getWxOpenId(code);
        if(data!=null){
            return JSONMessage.success(data);
        }else{
            return JSONMessage.failureByErrCode(KConstants.ResultCode.GetOpenIdFailure);
        }
    }



    @ApiOperation("设置隐藏会话密码")
    @ApiImplicitParam(paramType="query" , name="password",value="隐藏会话密码",dataType="String")
    @RequestMapping(value ="/user/setHideChatPassword")
    public JSONMessage setHideChatPassword(@RequestParam(defaultValue = "") String password){
        Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());
        authKeysService.setHideChatPassword(userId,password);
        MultipointSyncUtil.multipointLoginDataSync(userId, userCoreService.getNickName(userId), MultipointSyncUtil.MultipointLogin.SYNC_PRIVATE_CHAT_PASSWORD, messageBean -> {
            messageBean.setContent(password);
            return messageBean;
        });
        return JSONMessage.success();
    }
    @ApiOperation(value = "注销当前登陆用户，删除所有用户数据")
    @RequestMapping(value = "/user/destroy")
    public JSONMessage destroy() {
        try {
            userManager.deleteUser(1000,ReqUtil.getUserId().toString());
            return JSONMessage.success();
        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }
    }
}
