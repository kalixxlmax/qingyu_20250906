package com.basic.im.user.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.Role;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface RoleDao extends IMongoDAO<Role, ObjectId> {

    Role getUserRoleByUserId(Integer userId);

    Role getUserRole(Integer userId,String phone,Integer type);

    List<Role> getUserRoleList(Integer userId,String phone,Integer type);

    boolean queryUserRoleIsExist(Integer userId, int role);

    PageResult<Role> getAdminRoleList(String keyWorld, int page, int limit, Integer type, Integer userId,String roleId);

    void setAdminRole(String telePhone, String phone, byte role, Integer type);

    void deleteAdminRole(Integer userId,Integer type);

    Role updateRole(Role role);

    Role updateRole(int userId, byte role, Map<String,Object> map);

    void deleteRole(Integer userId);

    void addRole(Role role);

    Role getRoleByUserId(Integer userId);

    void updateUserRole(ObjectId id,String roleId,String roleName);

    void updateUserRole(Integer id,List<String> resourceIdList);

    List<Role> queryAllRole();

    void updateUserRole(int userId,String roleId);
}
