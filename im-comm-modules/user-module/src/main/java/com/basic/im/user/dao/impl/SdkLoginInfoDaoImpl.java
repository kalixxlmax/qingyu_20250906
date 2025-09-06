package com.basic.im.user.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.SdkLoginInfoDao;
import com.basic.im.user.model.SdkLoginInfo;
import com.basic.utils.DateUtil;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class SdkLoginInfoDaoImpl extends MongoRepository<SdkLoginInfo,ObjectId> implements SdkLoginInfoDao {

    @Override
    public Class<SdkLoginInfo> getEntityClass() {
        return SdkLoginInfo.class;
    }

    @Override
    public SdkLoginInfo addSdkLoginInfo(int type, Integer userId, String loginInfo) {
        SdkLoginInfo entity=new SdkLoginInfo();
        entity.setType(type);
        entity.setLoginInfo(loginInfo);
        entity.setUserId(userId);
        entity.setCreateTime(DateUtil.currentTimeSeconds());
        getDatastore().save(entity);
        return entity;
    }
    @Override
    public SdkLoginInfo addSdkLoginInfo(int type,String loginInfo, String userData) {
        SdkLoginInfo entity=new SdkLoginInfo();
        entity.setType(type);
        entity.setLoginInfo(loginInfo);
        entity.setCreateTime(DateUtil.currentTimeSeconds());
        entity.setUserData(userData);
        getDatastore().save(entity);
        return entity;
    }

    @Override
    public void deleteSdkLoginInfo(int type, Integer userId) {
        Query query =createQuery("type",type);
        addToQuery(query,"userId",userId);
        deleteByQuery(query);
    }

    @Override
    public List<SdkLoginInfo> querySdkLoginInfoByUserId(Integer userId) {
        Query query = createQuery("userId",userId);
        return queryListsByQuery(query);
    }

    @Override
    public SdkLoginInfo findSdkLoginInfo(int type, String loginInfo) {
        Query query=createQuery("type",type);
        addToQuery(query,"loginInfo",loginInfo);

        return findOne(query);
    }

    @Override
    public SdkLoginInfo querySdkLoginInfo(String loginInfo) {
        Query query=createQuery("loginInfo",loginInfo);
        return findOne(query);
    }

    @Override
    public SdkLoginInfo getSdkLoginInfo(int type, Integer userId) {

        Query query=createQuery("type",type);
        addToQuery(query,"userId",userId);
        return findOne(query);
    }

    @Override
    public PageResult<SdkLoginInfo> getSdkLoginInfoList(int pageIndex, int pageSize, String keyword) {
        PageResult<SdkLoginInfo> result = new PageResult<>();
        Query query = createQuery();
        if(!StringUtil.isEmpty(keyword)) {
            addToQuery(query,"userId",Integer.valueOf(keyword));
        }
        descByquery(query,"createTime");
        result.setCount(count(query));
        result.setData(queryListsByQuery(query,pageIndex,pageSize,1));
        return result;
    }

    @Override
    public void deleteSdkLoginInfo(ObjectId id) {
        deleteById(id);
    }

    @Override
    public void deleteSdkLoginInfoByUserId(int userId) {
        Query query=createQuery("userId", userId);
        deleteByQuery(query);
    }

    @Override
    public void initLoginInfoUserId(int type,String loginInfo, Integer userId) {

        Query query=createQuery("type",type);
        addToQuery(query,"loginInfo",loginInfo);
        SdkLoginInfo entity = findOne(query);
        if(null!=entity){
            Update update = createUpdate().set("userId", userId);
            getDatastore().updateFirst(query, update,getEntityClass());
        }else {
            entity=new SdkLoginInfo();
            entity.setType(type);
            entity.setUserId(userId);
            entity.setLoginInfo(loginInfo);
            entity.setCreateTime(DateUtil.currentTimeSeconds());
            getDatastore().save(entity);
        }
    }

    public void updateSdkLoginInfo(int type,String loginInfo,Integer userId){
        Query query=createQuery("type",type);
        addToQuery(query,"loginInfo",loginInfo);
        Update update = createUpdate().set("userId", userId);
        getDatastore().updateFirst(query, update,getEntityClass());
    }

}
