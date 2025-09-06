package com.basic.im.user.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.model.SdkLoginInfo;
import org.bson.types.ObjectId;

import java.util.List;

public interface SdkLoginInfoDao extends IMongoDAO<SdkLoginInfo,ObjectId> {

    SdkLoginInfo addSdkLoginInfo(int type, Integer userId, String loginInfo);

    SdkLoginInfo addSdkLoginInfo(int type, String loginInfo, String userData);

    void updateSdkLoginInfo(int type,String loginInfo,Integer userId);

    void deleteSdkLoginInfo(int type, Integer userId);

    List<SdkLoginInfo> querySdkLoginInfoByUserId(Integer userId);

    SdkLoginInfo findSdkLoginInfo(int type, String loginInfo);

    SdkLoginInfo querySdkLoginInfo(String loginInfo);

    SdkLoginInfo getSdkLoginInfo(int type, Integer userId);

    PageResult<SdkLoginInfo> getSdkLoginInfoList(int pageIndex, int pageSize, String keyword);

    void deleteSdkLoginInfo(ObjectId id);

    void deleteSdkLoginInfoByUserId(int userId);

    void initLoginInfoUserId(int type, String loginInfo, Integer userId);
}
