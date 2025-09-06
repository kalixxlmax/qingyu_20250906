package com.basic.im.user.dao.impl;

import com.mongodb.client.MongoCursor;
import com.basic.common.core.MongoOperator;
import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.AuthKeysDao;
import com.basic.im.user.entity.AuthKeys;
import com.basic.mongodb.wrapper.QueryWrapper;
import com.basic.mongodb.wrapper.UpdateWrapper;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/3 9:41
 */
@Repository
public class AuthKeysDaoImpl extends MongoRepository<AuthKeys,Integer> implements AuthKeysDao {

    @Override
    public Class<AuthKeys> getEntityClass() {
        return AuthKeys.class;
    }

    @Override
    public List<AuthKeys> getYopNotNull() {
        Query query = createQuery();
        query.addCriteria(Criteria.where("walletUserNo").ne(null));
        return queryListsByQuery(query);
    }

    @Override
    public void updateHideChatPassword(Integer userId, String password) {
        getDatastore().updateFirst(QueryWrapper.query(AuthKeys::getUserId,userId).build(),UpdateWrapper.update(AuthKeys::getHideChatPassword,password).build(),getEntityClass());
    }

    @Override
    public void addAuthKeys(AuthKeys authKeys) {
        getDatastore().save(authKeys);
    }

    @Override
    public AuthKeys getAuthKeys(int userId) {
        return get(userId);
    }

    @Override
    public AuthKeys queryAuthKeys(int userId) {
        Query query=createQuery("_id",userId);
        query.fields().exclude("dhMsgKeyList");
        return findOne(query);
    }

    @Override
    public boolean updateAuthKeys(int userId, Map<String, Object> map) {
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        return updateAttributeByOps(userId, ops);
    }

    @Override
    public Object queryOneFieldByIdResult(String key, int userId) {
        return queryOneFieldById(key, userId);
    }

    @Override
    public Map<String, String> queryUseRSAPublicKeyList(List<Integer> userList) {
        Document query=new Document();
        query.append("_id",new Document(MongoOperator.IN,userList));

        Document fields=new Document("_id",1).append("msgRsaKeyPair",1);
       /* Query query=createQuery();
        query.addCriteria(Criteria.where("_id").in(userList));
        query.fields().include("_id").include("msgRsaKeyPair");*/
        Map<String,String> result=new HashMap<>();

        MongoCursor<Document> iterator = mongoTemplate.getCollection(getCollectionName(getEntityClass())).find(query).projection(fields).iterator();
        try  {
            while (iterator.hasNext()){
                Document next = iterator.next();
                Document msgRsaKeyPair= (Document) next.get("msgRsaKeyPair");
                if(null==msgRsaKeyPair) {
                    continue;
                }else if(null!=msgRsaKeyPair.getString("publicKey")){
                    result.put(String.valueOf(next.getInteger("_id")),msgRsaKeyPair.getString("publicKey"));
                }

            }
        }finally {
            if(null!=iterator){
                iterator.close();
            }
        }
        return result;
    }
    @Override
    public List<Integer>  queryIsRsaAccountUserIdList() {
        Query query = createQuery();
        query.addCriteria(Criteria.where("msgDHKeyPair.publicKey").ne(null));

        return distinct("_id",query,Integer.class);
    }

    @Override
    public Document  queryMsgAndDHPublicKey(Integer userId) {
        Document query=new Document();
        query.append("_id",userId);
        query.append("msgRsaKeyPair.publicKey",new Document("$ne",null));
        query.append("msgDHKeyPair.publicKey",new Document("$ne",null));
        Document fields=new Document("_id",1)
                .append("msgRsaKeyPair.publicKey",1).append("msgDHKeyPair.publicKey",1);
       return mongoTemplate.getCollection(getCollectionName(getEntityClass())).find(query).projection(fields).first();
    }

    @Override
    public void deleteAuthKeys(int userId) {
        Query query = createQuery("_id",userId);
        deleteByQuery(query);
    }
}
