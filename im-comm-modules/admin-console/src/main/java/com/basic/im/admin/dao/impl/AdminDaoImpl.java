package com.basic.im.admin.dao.impl;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.AdminDao;
import com.basic.im.admin.entity.Admin;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.StringUtil;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/3 14:38
 */
@Repository
public class AdminDaoImpl extends MongoRepository<Admin, ObjectId> implements AdminDao {
    @Override
    public MongoTemplate getDatastore() {
        return super.getDatastore();
    }

    @Override
    public Class<Admin> getEntityClass() {
        return Admin.class;
    }

    @Override
    public void addAdmin(Admin admin) {
        getDatastore().save(admin);
    }

    @Override
    public Admin getAdminByAccount(String account) {
        return  queryOne("account", account);
    }

    @Override
    public Admin getAdminById(ObjectId adminId) {
        return get(adminId);
    }

    @Autowired(required = false)
    @Qualifier(value = "mongoMall")
    protected MongoTemplate mongoTemplateMall;

    @Override
    public PageResult<Admin> getAdminList(String keyWorld, ObjectId adminId, int pageIndex, int pageSize) {
        PageResult<Admin> result = new PageResult<Admin>();

        Query query = createQuery();
        query.addCriteria(Criteria.where("_id").ne(adminId));

         //排除自己
        if (!StringUtil.isEmpty(keyWorld)) {
            query.addCriteria(Criteria.where("account").regex("account",keyWorld));
        }
        query.with(Sort.by(Sort.Order.desc("createTime")));
        query.with(createPageRequest(pageIndex,pageSize));
        result.setData(queryListsByQuery(query));
        result.setCount(count(query));

        return result;
    }

    @Override
    public void deleteAdmin(ObjectId id) {

        deleteByQuery(createQuery("_id",id));
    }

    @Override
    public boolean updateAdminPassword(ObjectId adminId, String newPwd) {
        Admin admin =queryOne("_id",adminId);
        admin.setPassword(newPwd);
        if(getDatastore().save(admin)!=null) {
            return true;
        }
        return false;
    }

    @Override
    public Admin updateAdmin(ObjectId adminId, Map<String, Object> map) {
        Query query = createQuery("_id",adminId);
        Update ops =createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        return getDatastore().findAndModify(query,ops,getEntityClass());
    }

    @Override
    public void deleteShop(Long userId) {
        if (null == mongoTemplateMall){
            return;
        }
        MongoCollection<Document> dbCollection =  mongoTemplateMall.getCollection("shop");
        Document query = new Document();
        query.put("userId", userId);
        MongoCursor<Document> iterator = dbCollection.find(query).iterator();
        try {
            
            while (iterator.hasNext()) {
                Document dbObj = iterator.next();
                try {
                    ObjectId objectId = dbObj.getObjectId("_id");
                    deleteProduct(objectId.toString());
                } catch (Exception e) {
                }
            }
            dbCollection.deleteMany(query);
            deleteMember(userId);
        }finally {
            if(null!=iterator){
                iterator.close();
            }
        }
       
    }

    @Override
    public void deleteProduct(String shopId) {
        MongoCollection<Document> dbCollection =  mongoTemplateMall.getCollection("product");
        Document query = new Document();
        query.put("shopId", shopId);
        dbCollection.deleteMany(query);
    }

    @Override
    public void deleteMember(Long userId){
        MongoCollection<Document> dbCollection =  mongoTemplateMall.getCollection("member");
        Document query = new Document();
        query.put("_id", userId);
        dbCollection.deleteMany(query);
    }
}
