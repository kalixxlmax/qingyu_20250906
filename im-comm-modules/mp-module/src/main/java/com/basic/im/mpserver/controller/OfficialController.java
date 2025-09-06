package com.basic.im.mpserver.controller;

import cn.hutool.core.util.ObjectUtil;
import com.basic.im.mpserver.utils.BusinessUtils;
import com.basic.im.mpserver.utils.ValidateIDCardUtils;
import com.basic.im.open.entity.OfficialInfo;
import com.basic.im.security.entity.SecurityRole;
import com.basic.im.security.service.SecurityRoleManager;
import com.basic.im.sms.service.SMSServiceImpl;
import com.basic.im.user.dao.RoleDao;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.Role;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.DateUtil;
import com.basic.utils.Md5Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author xie yuan yang
 * @Date 2020/4/15
 **/
@RestController
@RequestMapping("/mp")
public class OfficialController {

    @Autowired
    private UserDao userDao;

    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private RoleDao roleDao;

    @Autowired
    private SecurityRoleManager securityRoleManager;

    @Autowired
    private SMSServiceImpl smsService;


    /**
     * @Description 注册账号
     * @Date 12:18 2020/4/15
     **/
    @RequestMapping(value = "/registerOfficial")
    public JSONMessage registerOfficial(@ModelAttribute OfficialInfo info,String randcode){
        String phone=info.getTelephone();
        info.setPhone(info.getTelephone());
        //验证验证码
        /*if(!smsService.isAvailable(info.getAreaCode()+info.getTelephone(),randcode)) {
            //return JSONMessage.failureByErrCode(KConstants.ResultCode.VerifyCodeErrOrExpired);
            return JSONMessage.failure("验证码错误或以过期！");
        }*/

        //校验身份证
        boolean flag = ValidateIDCardUtils.validateIdCard18(info.getAdminID());
        if (!flag){
            return JSONMessage.failure("管理员身份证格号码式错误！");
        }

        //设置创建时间
        info.setCreateTime(DateUtil.currentTimeSeconds());
        info.setTelephone(info.getAreaCode()+info.getTelephone());
        User user=userDao.getUser(info.getTelephone());
        if (user == null){
            //生成用户
            User newUser = userManager.createUser(info.getTelephone(),info.getPassword());

            newUser.setAreaCode(info.getAreaCode());
            newUser.setUserType(info.getOfficialType());
            newUser.setPhone(phone);
            newUser.setCityId(400300);
            newUser.setStatus(1);
            newUser.setLoc(new User.Loc(10.0,10.0));
            if (info.getOfficialType() == 2){
                newUser.setNickname("个人号");
            }else if(info.getOfficialType() == 4){
                newUser.setNickname("企业号");
            }
            //默认初始化配置
            newUser.setSettings(new User.UserSettings());
            newUser.setTelephone(info.getTelephone());
            newUser.setUserKey(Md5Util.md5Hex(info.getAreaCode()+phone));
            newUser.setCreateTime(DateUtil.currentTimeSeconds());
            newUser.setModifyTime(DateUtil.currentTimeSeconds());
            //保存到数据库
            User user1 = userManager.getUserDao().addUsers(newUser);

            //保存到数据库
            info.setUserId(user1.getUserId());
            SKBeanUtils.getDatastore().save(info);

            //创建角色用户
            Role role = new Role();
            role.setUserId(user1.getUserId());//用户编号
            role.setStatus((byte) 1);
            role.setPhone(info.getAreaCode()+phone);
            role.setRole((byte)2);
            role.setCreateTime(DateUtil.currentTimeSeconds());
            SecurityRole securityRole = securityRoleManager.querySecurityRoleByRoleName("公众号");
            role.setRoleId(String.valueOf(securityRole.getRoleId()));
            roleDao.save(role);

            //初始化配置
        }else{
            return JSONMessage.failure("账户已存在！");
        }
        return JSONMessage.success();
    }

    /**
     * 校验营业执照号
     * @param companyBusinessLicense
     * @return
     */
    @RequestMapping(value = "/isBusinessLicense")
    public JSONMessage isBusinessLicense(String companyBusinessLicense){
        //校验营业执照号
       /* if (companyBusinessLicense.length() == 15){
            boolean businessLicense15 = BusinessUtils.isBusinessLicense15(companyBusinessLicense);
            if (!businessLicense15){
                return JSONMessage.failure("营业执照号格式错误！");
            }
        }else if(companyBusinessLicense.length() == 18){
            boolean businessLicense18 = BusinessUtils.isBusinessLicense18(companyBusinessLicense);
            if (!businessLicense18){
                return JSONMessage.failure("统一社会信用代码格式错误！");
            }
        }else{
            return JSONMessage.failure("统一社会信用代码格式错误！");
        }*/
        return JSONMessage.success();
    }

    /**
     * 校验企身份证号码
     * @param identity
     * @return
     */
    @RequestMapping(value = "/isIdentity")
    public JSONMessage isIdentity(String identity){
        //校验身份证
        boolean flag = ValidateIDCardUtils.validateIdCard18(identity);
        if (!flag){
            return JSONMessage.failure("身份证号码格式错误！");
        }
        return JSONMessage.success();
    }


    @RequestMapping(value = "/checkInfo")
    public JSONMessage checkInfo(@ModelAttribute OfficialInfo info,String randcode){
        String phone=info.getTelephone();
        info.setPhone(info.getTelephone());
        try{
            //验证验证码
            if(!smsService.isAvailable(info.getAreaCode()+info.getTelephone(),randcode)) {
                return JSONMessage.failure("验证码错误或已过期！");
            }

            //校验身份证
            boolean flag = ValidateIDCardUtils.validateIdCard18(info.getAdminID());
            if (!flag){
                return JSONMessage.failure("管理员身份证格号码式错误！");
            }

            User user=userDao.getUser(info.getAreaCode()+info.getTelephone());
            if (!ObjectUtil.isEmpty(user)){
                return JSONMessage.failure("账户已存在！");
            }
            return JSONMessage.success();

        } catch (Exception e) {
            return JSONMessage.failureByException(e);
        }

    }

}
