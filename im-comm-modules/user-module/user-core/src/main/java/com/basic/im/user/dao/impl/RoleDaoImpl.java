package com.basic.im.user.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.RoleDao;
import com.basic.im.user.entity.Role;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Repository
public class RoleDaoImpl extends MongoRepository<Role, ObjectId> implements RoleDao {


    @Override
    public Class<Role> getEntityClass() {
        return Role.class;
    }

    @Override
    public Role getUserRoleByUserId(Integer userId) {
        return findOne("userId", userId);
    }

    @Override
    public Role getUserRole(Integer userId, String phone, Integer type) {
        Query query = createQuery();
        if(0 != userId) {
            addToQuery(query,"userId", userId);
        } else {
            if(!StringUtil.isEmpty(phone)) {
                addToQuery(query,"phone", phone);
            }
        }

        if(null != type && 5 == type){
            int num=type+1;
            query.addCriteria(Criteria.where("role").in(Arrays.asList(type,num,1,4,7)));

        }
        return findOne(query);
    }

    @Override
    public List<Role> getUserRoleList(Integer userId, String phone, Integer type) {
        Query query = createQuery();
        if(0 != userId) {
            addToQuery(query,"userId", userId);
        }
        if(!StringUtil.isEmpty(phone)) {
            addToQuery(query,"phone", phone);
        }
        if(null != type) {
            addToQuery(query,"role", type);
        }
        return queryListsByQuery(query);
    }

    @Override
    public boolean queryUserRoleIsExist(Integer userId,  int role) {
        Query query = createQuery("userId", userId);
        if(0 != role) {
            addToQuery(query,"role", role);
        }

        return exists(query);
    }

    @Override
    public PageResult<Role> getAdminRoleList(String keyWorld, int page, int limit, Integer type, Integer userId, String roleId) {
        Query query =createQuery();
        if(0 == type){
            query.addCriteria(Criteria.where("role").in(Arrays.asList(5,6)));
            query.addCriteria(Criteria.where("userId").ne(userId)); //排除自己
        }else if(4 == type) {
            addToQuery(query,"role",  4);// 客服
        } else if(7 == type) {
            addToQuery(query,"role",  7);// 财务
        } else if(3 == type) {
            addToQuery(query,"role",  3);// 机器人
        } else if(1 == type) {
            addToQuery(query,"role", 1);// 游客
        } else if(2 == type) {
            addToQuery(query,"role",  2);// 公众号
        } else if (8 == type){
            addToQuery(query,"role",  type);
            addToQuery(query,"roleId",  roleId);
        } else{
            addToQuery(query,"role",  type);
        }
        if (!StringUtil.isEmpty(keyWorld)) {
            query.addCriteria(containsIgnoreCase("phone",keyWorld));
        }
        descByquery(query,"createTime");
        PageResult<Role> pageResult = new PageResult<>();
        pageResult.setData(queryListsByQuery(query,page,limit,1));
        pageResult.setCount(count(query));
        return pageResult;
    }

    @Override
    public void setAdminRole(String telePhone, String phone, byte role, Integer type) {

    }

    @Override
    public void deleteAdminRole(Integer userId, Integer type) {
        Query query = createQuery("userId", userId);
        addToQuery(query,"role",userId);
        deleteByQuery(query);
    }

    @Override
    public Role updateRole(Role role) {
        Query query = createQuery("userId", role.getUserId());
        addToQuery(query,"role",role.getRole());
        Update ops = createUpdate();
        if(role.getRole() != 0) {
            ops.set("role", role.getRole());
        }

        if(role.getStatus() != 0) {
            ops.set("status", role.getStatus());
        }

        if(0 != role.getLastLoginTime()) {
            ops.set("lastLoginTime", role.getLastLoginTime());
        }
        if(!StringUtil.isEmpty(role.getPromotionUrl())){
            ops.set("promotionUrl", role.getPromotionUrl());
        }
        return getDatastore().findAndModify(query, ops,getEntityClass());
    }

    @Override
    public void deleteRole(Integer userId) {
      deleteByAttribute("userId",userId);
    }

    @Override
    public void addRole(Role role) {
        getDatastore().save(role);
    }

    @Override
    public Role getRoleByUserId(Integer userId) {
        Query query = createQuery("userId", userId);
        return findOne(query);
    }

    @Override
    public void updateUserRole(ObjectId id, String roleId, String roleName) {
        Query query = createQuery(id);
        Update ops = createUpdate();
        ops.set("roleId",roleId);
        ops.set("roleName",roleName);
        getDatastore().findAndModify(query,ops,getEntityClass());
    }

    @Override
    public void updateUserRole(Integer userId, List<String> resourceIdList) {
        Query query = createQuery("userId",userId);
        Update ops = createUpdate();
        ops.set("resourceIdList",resourceIdList);
        getDatastore().findAndModify(query,ops,getEntityClass());
    }

    @Override
    public List<Role> queryAllRole() {
        return queryListsByQuery(createQuery());
    }

    @Override
    public Role updateRole(int userId, byte role, Map<String, Object> map) {
        Query query =  createQuery("userId", userId);
        addToQuery(query,"role",role);
		Update ops = createUpdate();
		map.forEach((key,value)->{
		    ops.set(key,value);
        });
        return getDatastore().findAndModify(query,ops,getEntityClass());
    }

    @Override
    public void updateUserRole(int userId ,String roleId) {
        Query query = createQuery("userId",userId);
        Update ops = createUpdate();
        ops.set("roleId",roleId);
        getDatastore().findAndModify(query,ops,getEntityClass());
    }
}
