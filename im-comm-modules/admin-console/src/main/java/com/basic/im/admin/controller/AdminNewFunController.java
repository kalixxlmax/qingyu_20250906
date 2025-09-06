package com.basic.im.admin.controller;

import com.alibaba.fastjson.JSONObject;
import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.DisCoverAppDao;
import com.basic.im.admin.entity.*;
import com.basic.im.admin.jedis.AdminRedisRepository;
import com.basic.im.admin.service.AdminManager;
import com.basic.im.admin.service.DisCoverAppManager;
import com.basic.im.admin.service.PushManager;
import com.basic.im.admin.service.SecretQuestionManager;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.entity.PushConfig;
import com.basic.im.entity.SmsConfig;
import com.basic.im.sms.service.SMSServiceImpl;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.service.UserManager;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName AdminConfigController
 * @Author xie yuan yang
 * @date 2020.11.11 09:40
 * @Description
 */
@ApiIgnore
@RestController
@Slf4j
@RequestMapping("/console")
public class AdminNewFunController {

    @Autowired
    private SecretQuestionManager secretQuestionManager;

    @Autowired
    private DisCoverAppManager disCoverAppManager;

    @Autowired
    private DisCoverAppDao disCoverAppDao;

    @Autowired
    private AdminRedisRepository adminRedisRepository;

    @Autowired
    private PushManager pushManager;

    @Autowired
    private AdminManager adminManager;

    @Autowired
    private SMSServiceImpl smsService;

    @Autowired
    private UserManager userManager;

    @Autowired
    private UserDao userDao;

    @Autowired
    private UserCoreRedisRepository userCoreRedisRepository;


    /**
     * 获取密保问题列表
     **/
    @RequestMapping(value = "/list/secret/question")
    public JSONMessage getSecretQuestionList(int page,int limit,String keyword) {
        return JSONMessage.success(secretQuestionManager.getListSecretQuestion(page, limit, keyword));
    }

    /**
     * 删除密保问题
     **/
    @RequestMapping(value = "/del/secret/question")
    public JSONMessage deleteSecretQuestionById(String id) {
        secretQuestionManager.deleteSecretQuestionById(new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * 添加密保问题
     **/
    @RequestMapping(value = "/create/secret/question")
    public JSONMessage addSecretQuestion(String question,byte status) {
        SecretQuestion sava = secretQuestionManager.sava(question, status);
        return JSONMessage.success(sava);
    }

    /**
     * 发现页列表
     **/
    @RequestMapping(value = "/getDisCoverPageList")
    public JSONMessage getDisCoverPageLis(int page, int limit , String keyword) {
        return JSONMessage.success(disCoverAppManager.getDisCoverApp(page, limit, keyword));
    }

    /**
     * 删除发现页
     **/
    @RequestMapping(value = "/deleteDisCoverPage")
    public JSONMessage deleteDisCoverPage(String id) {
        disCoverAppManager.deleteDisCoverApp(new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * 修改发现页状态
     **/
    @RequestMapping(value = "/getDisCoverPageLis")
    public JSONMessage getDisCoverPageLis(byte isShow,String id) {
        disCoverAppManager.updateDisCoverAppIsShow(isShow,new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * 修改发现页
     **/
    @RequestMapping(value = "/updateDisCoverApp")
    public JSONMessage updateDisCoverApp(DisCoverApp disCoverApp) {
        List<DisCoverApp> disCoverBySequence = disCoverAppDao.findDisCoverBySequence(disCoverApp.getSequence());
        if (disCoverBySequence.size() > 1){
            return JSONMessage.failure("顺序号以存在其他发现页中！");
        }
        disCoverAppManager.updateDisCoverApp(disCoverApp);
        return JSONMessage.success();
    }


    /**
     * 添加发现页
     **/
    @RequestMapping(value = "/addDisCoverApp")
    public JSONMessage addDisCoverApp(DisCoverApp disCoverApp) {
        List<DisCoverApp> disCoverBySequence = disCoverAppDao.findDisCoverBySequence(disCoverApp.getSequence());
        if (disCoverBySequence != null && disCoverBySequence.size() > 0){
            return JSONMessage.failure("顺序号以存在其他发现页中！");
        }
        disCoverAppManager.addDisCoverApp(disCoverApp);
        return JSONMessage.success();
    }


    /**
     * 推送测试
     * pushDevice -> 推送那些设备
     * title  -> 通知标题
     * text  -> 显示内容
     * pushToken -> 推送token
     * pushPackageName ->  推送包名
     **/
    @RequestMapping(value = "/pushDevice")
    public JSONMessage pushDevice(String pushDevice,String title,String text,String pushToken,String pushPackageName) {
        try {
            pushManager.pushDevice(pushDevice,title,text,pushToken,pushPackageName);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return JSONMessage.success();
    }


    /**
     * 获取所有推送信息
     **/
    @ApiOperation("获取所有推送信息")
    @RequestMapping(value = "/get/pushconfigmodel/list")
    public JSONMessage getPushConfigModelList() {
        List<PushConfig> pushConfigList = pushManager.getPushConfigList();
        return JSONMessage.success(pushConfigList);
    }

    /**
     * 新增推送配置
     **/
    @ApiOperation("新增推送配置")
    @RequestMapping(value = "/add/pushconfigmodel")
    public JSONMessage addPushConfigModel(PushConfigModelVO pushConfigModelVO) {
        PushConfig pushConfig = new PushConfig();
        if (0 != pushConfigModelVO.getId()){
            pushConfig = pushConfigModelVO.byPushConfig(pushConfigModelVO);
        }
        PushConfig result = pushManager.addPushConfig(pushConfig);
        return JSONMessage.success(result);
    }


    /**
     * 获取推送详情 根据Id
     **/
    @ApiOperation("获取推送详情 根据Id")
    @RequestMapping(value = "/get/pushconfigmodel/detail")
    public JSONMessage getPushConfigModelDetail(int id) {
        PushConfig result = pushManager.getPushConfigModelDetail(id);
        return JSONMessage.success(result);
    }


    /**
     * 删除推送配置 根据Id
     **/
    @ApiOperation("删除推送配置 根据Id")
    @RequestMapping(value = "/delete/pushconfigmodel")
    public JSONMessage deletePushConfig(int id) {
        boolean b = pushManager.deletePushConfig(id);
        return JSONMessage.success(b);
    }

    /**
     * @Description 获取资源服务器 文件列表
     * @Date 14:37 2020/10/20
     **/
    @RequestMapping(value = "/get/resource/list")
    public JSONMessage getResourceList(@RequestParam(defaultValue = "1") int page, @RequestParam(defaultValue = "15") int limit,
                                       @RequestParam(defaultValue = "") String keyword, @RequestParam(defaultValue = "") String fileType,
                                       @RequestParam(defaultValue = "") String startTime, @RequestParam(defaultValue = "") String endTime) {

        PageResult<UploadItem> result = adminManager.listResource(page, limit, keyword,fileType,startTime,endTime);
        return JSONMessage.success(result);
    }

    /**
     * @Description 删除资源
     * @Date 10:24 2020/10/21
     **/
    @RequestMapping(value = "/delete/resource")
    public JSONMessage deleteResource(@RequestParam(defaultValue = "") String id) {
        adminManager.deleteResource(id);
        return JSONMessage.success();
    }

    /**
     * @Description 管理员日志
     * @Date 16:10 2020/11/9
     **/
    @RequestMapping(value = "/person/operation/log")
    public JSONMessage getOperationLog(@RequestParam(defaultValue = "1") int page, @RequestParam(defaultValue = "15") int limit) {
        PageResult<OperationLog> result = new PageResult<>();
        List<OperationLog> data = new ArrayList<>();
        List<OperationLog> operationLog = adminRedisRepository.getOperationLog(ReqUtil.getUserId());
        int pageIndex = page * limit;
        int pageSize = page * limit + limit;
        int flag = pageSize > operationLog.size() ? operationLog.size() : pageSize;
        for (int i = pageIndex; i < flag; i++) {
            data.add(operationLog.get(i));
        }
        result.setData(data);
        result.setCount(flag);
        return JSONMessage.success(result);
    }

    /**
     * 发送系统通知
     * @param type
     * @param body
     * @return
     */
    @RequestMapping(value = "/sendSysNotice")
    public JSONMessage sendSysNotice(@RequestParam(defaultValue="0") Integer type,@RequestParam(defaultValue="") String body,@RequestParam(defaultValue="") String title,@RequestParam(defaultValue="") String url){
        try {
            if(StringUtil.isEmpty(body) || StringUtil.isEmpty(title)) {
                return JSONMessage.failure("标题或内容不能为空");
            }
            pushManager.sendSysNotice(type, body, title,url);
            return JSONMessage.success();
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure(e.getMessage());
        }
    }

    /**
     * @Description 获取推送内容列表
     * @Date 16:12 2020/11/9
     **/
    @RequestMapping(value = "/get/pushnews/list")
    public JSONMessage getPushNewsList(@RequestParam(defaultValue = "") String startTime,@RequestParam(defaultValue = "") String endTime,
                                       @RequestParam(defaultValue = "1") int page,@RequestParam(defaultValue = "15") int limit,
                                       @RequestParam(defaultValue = "") String type,@RequestParam(defaultValue = "")String content){
        PageResult<PushNews> pushNewsList = pushManager.getPushNewsList(startTime, endTime, page, limit, type,content);
        return JSONMessage.success(pushNewsList);
    }

    /**
     * @Description 删除推送
     * @Date 10:10 2020/11/10
     **/
    @RequestMapping(value = "/delete/pushnews")
    public JSONMessage deletePushNews(String id){
        pushManager.deletePushNews(id);
        return JSONMessage.success();
    }

    /**
     * 查询 OpenAdminLoginCode
     */
    @RequestMapping("/find/openAdminLoginCode")
    public JSONMessage isSendSms(){
        return JSONMessage.success(SKBeanUtils.getSystemConfig().getOpenAdminLoginCode());
    }


    @RequestMapping("/find/googleVerification")
    public JSONMessage googleVerification(){
        return JSONMessage.success(SKBeanUtils.getSystemConfig().getGoogleVerification());
    }
    /**
     * 后台管理登录——发送手机短信验证码
     **/
    @RequestMapping("/randcode/sendSms")
    public JSONMessage randcodeSendSms(@RequestParam(defaultValue = "86") String areaCode,String account){
        Map<String, Object> params = new HashMap<>();
        areaCode = "86";
        if (SKBeanUtils.getSystemConfig().getOpenAdminLoginCode() == KConstants.ZERO){
            params.put("sendStatus", 0);
            return JSONMessage.success(params);
        }
        User user = null;
        int userId = transitionUserId(account);
        //获取手机号
        try {
            if (KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT == userId){
                user = userManager.getUser(1000);
                if (!user.getAreaCode().equals(areaCode)){
                    return JSONMessage.failure("请选择正确的区号");
                }
            }else{
                user = userManager.getUser(areaCode+account);
            }
            if (null == user || StringUtil.isEmpty(user.getTelephone())){
                params.put("sendStatus", -1);
                return JSONMessage.success(params);
            }
        }catch (Exception e){
            params.put("sendStatus", -1);
            return JSONMessage.success(params);
        }

        try {
            //发送短信
            String telephone = user.getUserId() == KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT ? user.getPhone() : user.getTelephone();
            String code = smsService.sendSmsToInternational(telephone, user.getAreaCode(), KConstants.DEFAULT_LANGUAGE_LOWERCASE, 1);
            log.info(" sms Code  {}", code);
            params.put("sendStatus", 200);
        }catch (Exception e){
            return JSONMessage.error(e);
        }
        return JSONMessage.success(params);
    }

    /**
     * 后台管理登录——短信验证码检验
     **/
    @RequestMapping("/check/sms")
    public JSONMessage checkSms(@RequestParam(defaultValue = "86") String areaCode ,String account,String code){
        Map<String,Object> data = new HashMap<>();
        areaCode = "86";
        int userId = transitionUserId(account);
        try {
            User user = userId == KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT ? userManager.getUser(1000) : userManager.getUser(areaCode+account);
            String telephone = user.getUserId() == 1000 ? user.getPhone() : user.getTelephone();
            if (!smsService.isAvailable(telephone, code)) {
                throw new ServiceException(KConstants.ResultCode.VerifyCodeErrOrExpired);
            }
            data.put("param",200);
        }catch (Exception e){
            e.printStackTrace();
        }

        return JSONMessage.success(data);
    }

    /**
     *  设置管理员手机号
     */
    @RequestMapping("/set/admin/phone")
    public JSONMessage setAdminPhone(@RequestParam(defaultValue = "86") String areaCode ,String phone,@RequestParam(defaultValue = "")String countryCode){
        userDao.updateUser(KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT, new HashMap<String, Object>(){{
            put("phone", phone);
            put("areaCode", "86");
            put("countryCode", countryCode);
        }});
        userCoreRedisRepository.deleteUserByUserId(KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT);
        return JSONMessage.success();
    }

    /**
     * 查询电话号码
     */
    @RequestMapping(value = "/find/admin/phone")
    public JSONMessage getAdminPhone(@RequestParam(defaultValue = "86") String areaCode ,String account) {
        areaCode = "86";
        int userId = transitionUserId(account);
        User user = userId == KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT ? userManager.getUser(1000) : userManager.getUser(areaCode+account);
        if (user == null){
            return JSONMessage.success(KConstants.ZERO);
        }
        if (user.getUserId() == KConstants.systemAccount.ADMIN_CONSOLE_ACCOUNT){
            return user.getAreaCode().equals(areaCode) ? JSONMessage.success(user.getPhone()) : JSONMessage.failure("请选择正确的区号");
        }

        return JSONMessage.success(user.getTelephone());
    }

    /**
     * account 转换int类型
     */
    public Integer transitionUserId(String account){
        int userId = 0;
        try {
            userId = Integer.valueOf(account) ;
        }catch (Exception e){
            userId = 0;
        }
        return userId;
    }
}

