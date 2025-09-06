package com.basic.im.security.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.security.entity.SecurityRole;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @Description: 角色相关业务
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
public interface SecurityRoleDao extends IMongoDAO<SecurityRole, ObjectId> {

    /**
     * 添加角色
     * @param  securityRole
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
     */
    PageResult<SecurityRole> querySecurityRole(int pageIndex, int pageSize);

    /**
     * 根据Id查询角色
     * @param id
     */
    SecurityRole querySecurityRoleById(ObjectId id);

    /**
     * 角色列表
     * @return
     */
    List<SecurityRole> querySecurityRol1e();

    /**
     * 昵称查找角色
     * @param name
     * @return
     */
    SecurityRole querySecurityRoleByRoleName(String name);

    /**
     * 根据角色类型查询 角色信息
     * @param roleType
     */
    SecurityRole querySecurityRoleNameByType(byte roleType);
}
