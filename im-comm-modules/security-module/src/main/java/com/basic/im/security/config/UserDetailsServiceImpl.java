package com.basic.im.security.config;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.config.AppConfig;
import com.basic.im.security.entity.ResourceInfo;
import com.basic.im.security.entity.SecurityRole;
import com.basic.im.security.service.AdminsManager;
import com.basic.im.security.service.ResourceInfoManager;
import com.basic.im.security.service.SecurityRoleManager;
import com.basic.im.user.entity.Role;
import com.basic.im.user.model.KSession;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.user.service.impl.RoleManagerImpl;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.checkerframework.checker.units.qual.K;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @Description: TODO （认证处理）
 * @Author xie yuan yang
 * @Date 2020/3/3
 **/

@Component
public class UserDetailsServiceImpl implements UserDetailsService {

    @Autowired
    private UserManagerImpl userManager;
    @Autowired
    private RoleManagerImpl roleManager;
    @Autowired
    private AppConfig appConfig;
    @Autowired
    private AdminsManager adminsManager;
    @Autowired
    private SecurityRoleManager securityRoleManager;
    @Autowired
    private ResourceInfoManager resourceInfoManager;

    @Autowired
    private UserRedisService userRedisService;

    @Override
    public UserDetails loadUserByUsername(String account) throws UsernameNotFoundException {
        final Integer code = 86;

        com.basic.im.user.entity.User user = userManager.getUser1(code + account);
        if (ObjectUtil.isEmpty(user)) {
            throw new ServiceException("用户不存在!");
        }

        Role userRole = roleManager.getRole(user.getUserId());
        if (ObjectUtil.isEmpty(userRole)) {
            throw new ServiceException("权限不足!");
        }

        if (KConstants.LOSE == userRole.getStatus()) {
            throw new ServiceException("您的账号已被禁用!");
        }

        Map<String, Object> tokenMap = KSessionUtil.adminLoginSaveToken(user.getUserId().toString(), null);
        HashMap<String, Object> resultMap = new HashMap<>(8);
        resultMap.put("access_Token", tokenMap.get("access_Token"));
        resultMap.put("userId", user.getUserId());
        resultMap.put("areaCode", code);
        resultMap.put("adminId", user.getTelephone());
        resultMap.put("account", user.getUserId() + "");
        resultMap.put("apiKey", appConfig.getApiKey());
        resultMap.put("role", userRole.getRole() + "");
        resultMap.put("nickname", user.getNickname());
        resultMap.put("registerInviteCode", adminsManager.getConfig().getRegisterInviteCode());
        // 维护最后登录时间
        updateLastLoginTime(user.getUserId());

        List<GrantedAuthority> grantedAuthorities = queryUserAuthList(account, user.getUserId(), resultMap);


        SKBeanUtils.getRedisCRUD().set(account+"_loginData", JSON.toJSONString(resultMap));
        return setUserDetails(account, user.getPassword(), grantedAuthorities);
    }

    /**
     * 获取角色，资源，权限
     * @param account
     * @param userId
     * @param map
     * @return
     */
    public List<GrantedAuthority>  queryUserAuthList (String account,Integer userId,HashMap<String, Object> map){
        Set<String> userAuthList=new HashSet<>();

        List<GrantedAuthority> authorityList = new ArrayList<>();

        try {
            //设置 1000 为最高管理员
            if (account.equals("1000")) {
                List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfo();
                for (ResourceInfo resourceInfo : resourceInfos) {
                    userAuthList.add(resourceInfo.getResourceAuth());
                }
                authorityList.add(new SimpleGrantedAuthority("ROLE_" + "1000"));
                return authorityList;
            }
            //获取该角色的资源权限
            Role userRole = roleManager.getUserRole(userId);
            if(null==userRole||StringUtil.isEmpty(userRole.getRoleId())) {
                return authorityList;
            }

            SecurityRole securityRole = securityRoleManager.querySecurityRoleById(new ObjectId(userRole.getRoleId()));
            //是否有角色
            if (securityRole == null) {
                return authorityList;
            }
            map.put("securityRoleStatus",securityRole.getStatus());
            //判断角色是否禁用
            if (securityRole.getStatus().equals("1")) {
                authorityList.add(new SimpleGrantedAuthority("ROLE_" + securityRole.getRoleName()));
            }else{
                //判断管理员是否把角色给禁用了。
                throw new ServiceException("您的角色已被禁用!");
            }

            //获得该角色的资源权限
            if (securityRole.getRoleResourceList() != null){
                for (String s : securityRole.getRoleResourceList()) {
                    if (StringUtil.isEmpty(s)) {
                        continue;
                    }
                    ResourceInfo resourceInfo = resourceInfoManager.queryResourceInfoById(new ObjectId(s));
                    if (resourceInfo == null||StringUtil.isEmpty(resourceInfo.getResourceAuth())){
                        continue;
                    }
                    userAuthList.add(resourceInfo.getResourceAuth());
                }
            }

            //获取用户的资源权限
            if (null==userRole.getResourceIdList()) {
                map.put("userAuth",userAuthList);
                return authorityList;
            }
            for (String s : userRole.getResourceIdList()) {
                if (StringUtil.isEmpty(s)) {
                    continue;
                }
                ResourceInfo resourceInfo = resourceInfoManager.queryResourceInfoById(new ObjectId(s));
                if (resourceInfo == null||StringUtil.isEmpty(resourceInfo.getResourceAuth())){
                    continue;
                }
                userAuthList.add(resourceInfo.getResourceAuth());

            }




        }catch (Exception e){
            e.printStackTrace();
        }

        //返回给前端的数据
        map.put("userAuth",userAuthList);
        return authorityList;
    }

        private User setUserDetails (String username, String password, List < GrantedAuthority > authorityList){
            return new User(username, password, authorityList);
        }

        private void updateLastLoginTime (Integer userId){
            Role role = new Role(userId);
            roleManager.modifyRole(role);
        }


}