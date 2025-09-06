package com.basic.im.user.service;

import com.basic.im.user.entity.AuthKeys;
import org.bson.Document;

import java.util.List;
import java.util.Map;

public interface AuthKeysService {

    List<AuthKeys> getYopNotNull();

    AuthKeys getAuthKeys(int userId);

    String getPayPublicKey(int userId);

    void cleanTransactionSignCode(int userId, String codeId);

    String queryTransactionSignCode(int userId, String codeId);


    Document queryMsgAndDHPublicKey(Integer userId);

    void deleteAuthKeys(int userId);

    public void save(AuthKeys authKeys);

    public void update(int userId, Map<String,Object> map);

    String queryLoginPassword(int userId);

    String getPayPassword(Integer userId);

    List<Integer>  queryIsRsaAccountUserIdList();
}
