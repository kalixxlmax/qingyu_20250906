package com.basic.im.security.service.Impl;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.common.model.PageResult;
import com.basic.im.security.dao.SecurityRoleDao;
import com.basic.im.security.entity.ResourceInfo;
import com.basic.im.security.entity.SecurityRole;
import com.basic.im.security.entity.SecurityRoleChild;
import com.basic.im.security.service.SecurityRoleManager;
import com.basic.im.user.entity.Role;
import com.basic.im.user.service.impl.RoleManagerImpl;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Description:
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
@Service
public class SecurityRoleManagerImpl implements SecurityRoleManager {

    @Autowired
    private SecurityRoleDao securityRoleDao;

    @Autowired
    private ResourceInfoManagerImpl resourceInfoManager;

    @Autowired
    private RoleManagerImpl roleManager;

    @Override
    public SecurityRole sava(SecurityRole securityRole) {
        return securityRoleDao.sava(securityRole);
    }

    @Override
    public void deleteRole(ObjectId id) {
        securityRoleDao.deleteRole(id);
    }

    @Override
    public void updateRole(SecurityRole securityRole) {
        securityRoleDao.updateRole(securityRole);
    }

    @Override
    public void updateRoleResource(ObjectId roleId, List<String> roleResourceList) {
        securityRoleDao.updateRoleResource(roleId,roleResourceList);
    }

    @Override
    public PageResult<SecurityRole> querySecurityRole(int pageIndex, int pageSize) {
        return securityRoleDao.querySecurityRole(pageIndex,pageSize);
    }

    @Override
    public SecurityRole querySecurityRoleById(ObjectId id) {
        return securityRoleDao.querySecurityRoleById(id);
    }

    @Override
    public List<SecurityRole> querySecurityRol1e() {
        return securityRoleDao.querySecurityRol1e();
    }

    @Override
    public SecurityRole querySecurityRoleByRoleName(String name) {
        return securityRoleDao.querySecurityRoleByRoleName(name);
    }

    @Override
    public List<SecurityRole> securityRoleManager(String defaultString) {
        List<SecurityRoleChild> securityRoleChildren = JSONObject.parseArray(defaultString, SecurityRoleChild.class);

        List<SecurityRole> securityRoles = new ArrayList<>();
        for (SecurityRoleChild securityRoleChild : securityRoleChildren) {
            SecurityRole securityRole = new SecurityRole();
            securityRole.setRoleId(new ObjectId(securityRoleChild.getRoleId()));
            securityRole.setRoleName(securityRoleChild.getRoleName());
            securityRole.setRoleDesc(securityRoleChild.getRoleDesc());
            securityRole.setRoleResourceList(securityRoleChild.getRoleResourceList());
            securityRole.setStatus(securityRoleChild.getStatus());
            securityRoles.add(securityRole);
        }
        return securityRoles;
    }

    @Override
    public SecurityRole querySecurityRoleNameByType(byte roleType) {
        SecurityRole securityRole = securityRoleDao.querySecurityRoleNameByType(roleType);
        return securityRole;
    }

    /**
     * 维护角色权限数据（删除资源前操作）
     * @param resourceId
     */
    public void maintainSecurityRole(ObjectId resourceId){
        //资源详情
        ResourceInfo resourceInfo = resourceInfoManager.queryResourceInfoById(resourceId);
        //全部角色
        List<SecurityRole> securityRoles = querySecurityRol1e();

        securityRoles.forEach(securityRole -> {
            if (ObjectUtil.isEmpty(securityRole.getRoleResourceList())){
                return;
            }
            Boolean flag = false;
            //角色权限
            List<String> roleResourceList = securityRole.getRoleResourceList();
            for (int i = 0; i < roleResourceList.size(); i++) {
                if (roleResourceList.get(i).equals(String.valueOf(resourceInfo.getId()))) {
                    roleResourceList.remove(i);
                    flag = true;
                }
            }

            if (flag) {
                updateRoleResource(securityRole.getRoleId(), roleResourceList);
                flag = false;
            }
        });
    }

    /**
     * 维护用户权限资源数据（删除资源前操作）
     * @param resourceId
     */
    public void maintainRole(ObjectId resourceId){
        //资源详情
        ResourceInfo resourceInfo = resourceInfoManager.queryResourceInfoById(resourceId);
        //所有角色
        List<Role> roles = roleManager.queryAllRole();
        roles.forEach(role -> {
            boolean flag = false;
            //查询全部用户权限
            List<String> resourceIdList = role.getResourceIdList();
            if (ObjectUtil.isEmpty(resourceIdList)) {
               return;
            }
            for (int i = 0; i < resourceIdList.size(); i++) {
                if (String.valueOf(resourceIdList.get(i)).equals(String.valueOf(resourceInfo.getId()))) {
                    resourceIdList.remove(i);
                    flag = true;
                }
            }

            if (flag) {
                roleManager.updateUserRole(role.getUserId(), resourceIdList);
                flag = false;
            }
        });
    }

    /**
     * 获取用户中权限
     * @param userId
     * @param role
     * @return
     */
    public Set<String> getUserResource(Role role, int userId) {
        Set<String> result = new HashSet<>();
        //用户的权限
        if (ObjectUtil.isEmpty(role.getResourceIdList())){
            return result;
        }

        for (String resourceId : role.getResourceIdList()) {
            result.add(resourceId);
        }
        return result;
    }

    /**
     * 获取角色权限
     * @param role
     * @param userId
     * @return
     */
    public Set<String> getRoleResource(Role role, int userId) {
        Set<String> result = new HashSet<>();
        SecurityRole securityRole = querySecurityRoleById(new ObjectId(role.getRoleId()));
        if (ObjectUtil.isEmpty(securityRole.getRoleResourceList())){
            return result;
        }
        for (String resourceId : securityRole.getRoleResourceList()) {
            result.add(resourceId);
        }
        return result;
    }
}
