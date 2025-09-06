package com.basic.im.security.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.security.entity.ResourceInfo;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
public interface ResourceInfoDao  extends IMongoDAO<ResourceInfo, ObjectId> {

    /**
     * 查询全部资源
     **/
    List<ResourceInfo> queryResourceInfo();

    /**
     * 删除资源
     **/
    void delResourceInfo(ObjectId id);

    /**
     * 修改资源
     **/
    void updateResourceInfo(ResourceInfo resourceInfo);

    /**
     * 添加资源
     **/
    ResourceInfo sava(ResourceInfo resourceInfo);

    /**
     * 根据pid查询资源记录
     **/
    List<ResourceInfo> queryResourceInfoByPid(String pid);

    /**
     * 根据id查询资源记录
     **/
    ResourceInfo queryResourceInfoById(ObjectId id);

    /**
     * 根据名称查询资源
     **/
    List<ResourceInfo> queryResourceInfoByResourceName(String resourceName);

    List<ResourceInfo> queryResourceInfoByResourceUrl(String resourceUrl);

    List<ResourceInfo> queryResourceInfoByResourceAuth(String resourceAuth);
}