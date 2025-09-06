package com.basic.im.security.service;

import com.basic.common.model.PageResult;
import com.basic.im.security.entity.SecurityRole;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * @Description: 角色操作
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
public interface  SecurityRoleManager {

    /**
     * 添加角色
     * @param securityRole
     * @return
     */
    SecurityRole sava(SecurityRole securityRole);

    /**
     * 删除角色
     * @param id
     */
    void deleteRole(ObjectId id);

    /**
     * 修改角色
     * @param securityRole
     */
    void updateRole(SecurityRole securityRole);

    /**
     * 修改角色资源权限
     * @param roleId
     * @param roleResourceList
     */
    void updateRoleResource(ObjectId roleId, List<String> roleResourceList);

    /**
     * 查询全部角色
     * @param pageIndex
     * @param pageSize
     * @return
     */
    PageResult<SecurityRole> querySecurityRole(int pageIndex, int pageSize);


    /**
     * 根据Id查询角色
     * @param id
     * @return
     */
    SecurityRole querySecurityRoleById(ObjectId id);

    /**
     * 查询全部角色
     */
    List<SecurityRole> querySecurityRol1e();

    SecurityRole querySecurityRoleByRoleName(String name);

    List<SecurityRole> securityRoleManager(String defaultString);

    /**
     * 根据角色类型 查询用户信息
     * @param roleType
     * @return
     */
    SecurityRole querySecurityRoleNameByType(byte roleType);
}
