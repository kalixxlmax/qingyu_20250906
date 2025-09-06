package com.basic.im.open.dao;

import com.basic.common.model.PageResult;
import com.basic.im.open.opensdk.entity.SkOpenApp;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface SkOpenAppDao extends IMongoDAO<SkOpenApp, ObjectId> {

    void addSkOpenApp(SkOpenApp skOpenApp);

    SkOpenApp getSkOpenApp(ObjectId id);

    SkOpenApp getSkOpenApp(String appId);

    SkOpenApp getSkOpenApp(String appName,byte appType);

    PageResult<SkOpenApp> getSkOpenAppList(int status, int type, int pageIndex, int limit, String keyword);

    void deleteSkOpenApp(ObjectId id,String accountId);

    List<SkOpenApp> getSkOpenAppList(String accountId,int appType,int pageIndex,int pageSize);

    void updateSkOpenApp(ObjectId id,String accountId, Map<String,Object> map);

    SkOpenApp findByAppIdAndSecret(String appId,String secret);

    List<SkOpenApp> applicationList(String accountId,int appType,int pageIndex,int pageSize, String keyword);
}
