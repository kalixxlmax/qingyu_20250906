package com.basic.im.security.dao.Impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.security.dao.ResourceInfoDao;
import com.basic.im.security.entity.ResourceInfo;
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
public class ResourceInfoDaoImpl extends MongoRepository<ResourceInfo, ObjectId> implements ResourceInfoDao {
    @Override
    public Class<ResourceInfo> getEntityClass() {
        return ResourceInfo.class;
    }

    @Override
    public List<ResourceInfo> queryResourceInfo() {
        Query query = createQuery();
        return queryListsByQuery(query);
    }

    @Override
    public void delResourceInfo(ObjectId id) {
        deleteByQuery(createQuery(id));
    }

    @Override
    public void updateResourceInfo(ResourceInfo resourceInfo) {
        Query query = createQuery(resourceInfo.getId());
        Update ops = createUpdate();
        ops.set("resourceName",resourceInfo.getResourceName());
        ops.set("resourceUrl",resourceInfo.getResourceUrl());
        ops.set("resourceAuth",resourceInfo.getResourceAuth());
        ops.set("type",resourceInfo.getType());
        ops.set("status",resourceInfo.getStatus());
        ops.set("pid",resourceInfo.getPid());
        update(query,ops);
    }

    @Override
    public ResourceInfo sava(ResourceInfo resourceInfo) {


        ResourceInfo save = getDatastore().save(resourceInfo);
        return save;
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByPid(String pid) {
        Query query = createQuery("pid", pid);
        return queryListsByQuery(query);
    }

    @Override
    public ResourceInfo queryResourceInfoById(ObjectId id) {
        Query query = createQuery(id);
        return findOne(query);
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByResourceName(String resourceName) {
        Query query = createQuery("resourceName", resourceName);
        return queryListsByQuery(query);
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByResourceUrl(String resourceUrl) {
        Query query = createQuery("resourceUrl", resourceUrl);
        return queryListsByQuery(query);
    }

    @Override
    public List<ResourceInfo> queryResourceInfoByResourceAuth(String resourceAuth) {
        Query query = createQuery("resourceAuth", resourceAuth);
        return queryListsByQuery(query);
    }

}
