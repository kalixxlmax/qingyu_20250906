package com.basic.im.user.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.AuthKeys;
import org.bson.Document;

import java.util.List;
import java.util.Map;

public interface AuthKeysDao extends IMongoDAO<AuthKeys, Integer> {

    void addAuthKeys(AuthKeys authKeys);

    AuthKeys getAuthKeys(int userId);

    AuthKeys queryAuthKeys(int userId);

    boolean updateAuthKeys(int userId, Map<String,Object> map);

    Object queryOneFieldByIdResult(String key,int userId);

    Map<String,String> queryUseRSAPublicKeyList(List<Integer> userList);

    List<Integer>  queryIsRsaAccountUserIdList();

    Document queryMsgAndDHPublicKey(Integer userId);

    void deleteAuthKeys(int userId);

    List<AuthKeys> getYopNotNull();

    void updateHideChatPassword(Integer userId, String password);
}
