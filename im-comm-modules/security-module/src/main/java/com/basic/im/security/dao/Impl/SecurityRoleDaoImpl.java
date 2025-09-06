package com.basic.im.security.dao.Impl;

import com.basic.common.model.PageResult;
import com.basic.im.repository.MongoRepository;
import com.basic.im.security.dao.SecurityRoleDao;
import com.basic.im.security.entity.SecurityRole;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
@Repository
public class SecurityRoleDaoImpl extends MongoRepository<SecurityRole, ObjectId> implements SecurityRoleDao {
    @Override
    public Class<SecurityRole> getEntityClass() {
        return SecurityRole.class;
    }

    @Override
    public SecurityRole sava(SecurityRole securityRole) {
        SecurityRole save = getDatastore().save(securityRole);
        return save;
    }

    @Override
    public void deleteRole(ObjectId id) {
        deleteByQuery(createQuery(id));
    }

    @Override
    public void updateRole(SecurityRole securityRole) {
        Query query = createQuery(securityRole.getRoleId());
        Update ops = createUpdate();
        ops.set("roleName",securityRole.getRoleName());
        ops.set("roleDesc",securityRole.getRoleDesc());
        ops.set("status",securityRole.getStatus());
        update(query,ops);
    }

    @Override
    public void updateRoleResource(ObjectId roleId, List<String> roleResourceList) {
        Query query = createQuery(roleId);
        Update ops = createUpdate();
        ops.set("roleResourceList",roleResourceList);
        update(query,ops);
    }

    @Override
    public PageResult<SecurityRole> querySecurityRole(int pageIndex,int pageSize) {
        PageResult<SecurityRole> result = new PageResult<SecurityRole>();
        Query query = createQuery();
        query.with(createPageRequest(pageIndex-1,pageSize));
        result.setData(queryListsByQuery(query));
        result.setCount(count(query));
        return result;
    }

    @Override
    public SecurityRole querySecurityRoleById(ObjectId id) {
        Query query = createQuery(id);
        return findOne(query);
    }

    @Override
    public List<SecurityRole> querySecurityRol1e() {
        return queryListsByQuery(createQuery());
    }

    @Override
    public SecurityRole querySecurityRoleByRoleName(String name) {
        Query query = createQuery("roleName",name);
        return findOne(query);
    }

    @Override
    public SecurityRole querySecurityRoleNameByType(byte roleType) {
        Query query = createQuery("role",roleType);
        return findOne(query);
    }
}
