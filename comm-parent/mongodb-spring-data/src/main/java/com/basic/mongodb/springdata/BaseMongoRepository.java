package com.basic.mongodb.springdata;

import com.alibaba.fastjson.JSON;
import com.mongodb.ClientSessionOptions;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public abstract class BaseMongoRepository<T, ID extends Serializable> implements IBaseMongoRepository<T,ID> {


    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected static final Integer MIN_USERID=100000;

    protected static final Integer DB_REMAINDER=10000;

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Autowired
    protected MongoClient mongoClient;

    protected static final String ID_KEY="_id";

    @Override
    public MongoTemplate getDatastore(){
        return mongoTemplate;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }


    private Class<T> entityClass;


    public  Class<T> getEntityClass(){
        if(null==entityClass){
            Type type = getClass().getGenericSuperclass();
            Type trueType = ((ParameterizedType) type).getActualTypeArguments()[0];
            this.entityClass = (Class<T>) trueType;;
        }
        return entityClass;

    }


    /**
     * 获取 分库  分表 表名  分表 逻辑需要继承实现
     * 分表 的类 必须实现 这个方法
     * @param userId
     * @return
     */
    public String getCollectionName(int userId) {
        int remainder=0;
        if(userId>MIN_USERID) {
            remainder=userId/DB_REMAINDER;
        }
        return String.valueOf(remainder);
    }


    public String getCollectionName(ObjectId id) {
        if (null == id) {
            logger.info(" ====  getCollectionName ObjectId is null  ====");
            throw new RuntimeException("ObjectId  is  null !");
        } else {
            Integer remainder = 0;
            Integer counter = id.getCounter();
            remainder = counter /DB_REMAINDER;
            return String.valueOf(remainder);
        }
    }

    public static MongoCollection<Document> getCollection(MongoDatabase database, Integer userId) {
        Integer remainder=0;
        if(userId>BaseMongoRepository.MIN_USERID) {
            remainder=userId/DB_REMAINDER;
        }
        return database.getCollection(String.valueOf(remainder));
    }
    public static String getRemainderName(Integer userId) {
        Integer remainder=0;
        if(userId>MIN_USERID) {
            remainder=userId/DB_REMAINDER;
        }
        return String.valueOf(remainder);
    }


    /**
     * 获取 分库  分表  MongoCollection
     */
    @Override
    public MongoCollection<Document> getCollection(long userId) {
        String collectionName = getCollectionName(Math.toIntExact(userId));
        return getDatastore().getCollection(collectionName);
    }

    public MongoCollection<Document> getMongoCollection(MongoDatabase database,int userId) {
        Integer remainder=0;
        if(userId>MIN_USERID) {
            remainder=userId/DB_REMAINDER;
        }
        return database.getCollection(String.valueOf(remainder));
    }

    public MongoCollection<Document> getCollection(ObjectId id) {
        String collectionName = getCollectionName(id);

        return getDatastore().getCollection(collectionName);
    }


    @Override
    public MongoCollection<Document> getMongoCollection(String dbName) {

        return getDatastore().getCollection(dbName);
    }

    /**
     * 获取 分库  分表  MongoCollection<DBObject>
     * @param userId
     * @return
     */
    @Override
    public MongoCollection<DBObject> getDBObjectCollection(int userId) {

        String collectionName = getCollectionName(userId);

        return getDatastore().getDb().getCollection(collectionName,DBObject.class);
    }

    /**
     * 旧版操作 DBObject
     */
    public MongoCollection<DBObject> getDBObjectCollection(ObjectId id) {
        String collectionName = getCollectionName(id);
        return getDatastore().getDb().getCollection(collectionName, DBObject.class);
    }

    /**
     * 根据 用户 Id 即 取余 值  获取 实体表名
     * @param userId
     * @param remainder  取余值
     * @return
     */
    @Override
    public String getCollectionName(int userId, int remainder) {
        if(userId> MIN_USERID) {
            remainder=userId/remainder;
        }
        return String.valueOf(remainder);
    }




    /**
     * 获取  实体的表名
     * @return
     */
    @Override
    public String getCollectionName() {
        return getDatastore().getConverter().getMappingContext().getPersistentEntity(getEntityClass()).getCollection();

    }
    public String getCollectionName(Class zlass) {
        return getDatastore().getConverter().getMappingContext().getPersistentEntity(zlass).getCollection();

    }


    /**
     * 获取 当前 分表 库 下面的 表列表
     * @return
     */
    @Override
    public List<String> getCollectionList() {
        List<String> list = getDatastore().getCollectionNames()
                .stream().collect(Collectors.toList());
        list.remove("system.indexes");
        return list;
    }

    /**
     * 构建 startSession
     * @return
     */
    @Override
    public ClientSession startSession(){
        return getMongoClient().startSession();
    }

    @Override
    public ClientSession startSession(ClientSessionOptions sessionOptions){
        return getMongoClient().startSession(sessionOptions);
    }

    @Override
    public MongoTemplate withSession(ClientSession clientSession){
        return getDatastore().withSession(clientSession);
    }


    @Override
    public ClientSession startTransaction(){
        ClientSession clientSession = startSession();
        clientSession.startTransaction();
        return clientSession;
    }

    @Override
    public ClientSession startTransaction(ClientSession clientSession){
          clientSession.startTransaction();
          return clientSession;
    }








    @Override
    public Document objectToDocument(Object entity) {
        return Document.parse(JSON.toJSONString(entity));
    }
    @Override
    public Query createQuery(){
        return  new Query();
    }
    @Override
    public Query createQuery(ID id){
        return  new Query(Criteria.where("_id").is(id));
    }
    @Override
    public Query createQuery(String key, Object value){
       return  new Query(Criteria.where(key).is(value));
    }
    @Override
    public Query addToQuery(Query query, String key, Object value){
        return query.addCriteria(Criteria.where(key).is(value));
    }
    @Override
    public Update createUpdate(){
        return  new Update();
    }
    @Override
    public Criteria createCriteria(){
        return new Criteria();
    }
    @Override
    public Criteria createCriteria(String key, Object value){
        return Criteria.where(key).is(value);
    }
    @Override
    public Criteria contains(String key, String value){
        return Criteria.where(key).regex(value);
    }
    @Override
    public Criteria containsIgnoreCase(String key, String value){
        return Criteria.where(key).regex(Pattern.compile(value, Pattern.CASE_INSENSITIVE));
    }

    @Override
    public DeleteResult deleteById(ID id){
       return deleteByQuery(createQuery("_id",id));
    }
    @Override
    public DeleteResult deleteById(ID id, String collectionName){
        return deleteByQuery(createQuery("_id",id),collectionName);
    }


    @Override
    public DeleteResult deleteByAttribute(String key, Object value){
        return deleteByQuery(createQuery(key,value));
    }
    @Override
    public DeleteResult deleteByAttribute(String key, Object value, String collectionName){
        return deleteByQuery(createQuery(key,value),collectionName);
    }

    /**
     * 修改 当前实体  不为Null 的属性
     * 实体的属性 必须都是 引用类型  不然 属性会修改为默认值
     * @param id
     * @param entity
     */
    @Override
    public void updateAttribute(ID id,T entity) {
        Query query =new Query(Criteria.where("_id").is(id));
        Document document = objectToDocument(entity);
        Update update=new Update();
        document.keySet().forEach(key ->{
            if("_id".equals(key)||"createTime".equals(key)) {
                return;
            }
            update.set(key,document.get(key));
        });

        getDatastore().updateMulti(query,update,entity.getClass());
    }
    @Override
    public void updateAttribute(ID id, T entity, String collectionName) {
        Query query =new Query(Criteria.where("_id").is(id));
        Document document = objectToDocument(entity);
        Update update=new Update();
        document.keySet().forEach(key ->{
            if("_id".equals(key)||"createTime".equals(key)) {
                return;
            }
            update.set(key,document.get(key));
        });

        getDatastore().updateMulti(query,update,entity.getClass(),collectionName);
    }
    @Override
    public void updateAttributeByIdAndKey(ID id,String key,Object value) {
        Query query =new Query(Criteria.where("_id").is(id));
        Update update=Update.update(key,
                value);
        getDatastore().updateFirst(query,update,getEntityClass());
    }
    @Override
    public void updateAttributeByIdAndKey(ID id, String key, Object value, String collectionName) {
        Query query =new Query(Criteria.where("_id").is(id));
        Update update=Update.update(key,
                value);
        getDatastore().updateFirst(query,update,getEntityClass(),collectionName);
    }
    @Override
    public void updateAttributeByIdAndKey(Class<?> clazz,ID id,String key,Object value) {
        Query query =new Query(Criteria.where("_id").is(id));
        Update update=Update.update(key,
                value);
        getDatastore().updateFirst(query,update,clazz);
    }
    @Override
    public void updateAttributeByIdAndKey(Class<?> clazz, ID id, String key, Object value, String collectionName) {
        Query query =new Query(Criteria.where("_id").is(id));
        Update update=Update.update(key,
                value);
        getDatastore().updateFirst(query,update,clazz,collectionName);
    }
    //修改
    @Override
    public boolean updateAttributeByOps(ID id,Update update) {
        Query query =new Query(Criteria.where("_id").is(id));
        UpdateResult updateResult = getDatastore().updateMulti(query, update, getEntityClass());
        return 0<updateResult.getModifiedCount();
    }
    @Override
    public boolean updateAttributeByOps(ID id, Update update, String collectionName) {
        Query query =new Query(Criteria.where("_id").is(id));
        UpdateResult updateResult = getDatastore().updateMulti(query, update, getEntityClass(),collectionName);
        return 0<updateResult.getModifiedCount();
    }
    @Override
    public boolean updateAttribute(String queryStr,Object queryValue,String key,Object value) {
         Query query =new Query(Criteria.where(queryStr).is(queryValue));
        Update update=Update.update(key,value);
        UpdateResult updateResult = getDatastore().updateMulti(query, update, getEntityClass());

        return 0<updateResult.getModifiedCount();
    }
    @Override
    public void updateAttribute(String tbName, String queryStr,Object queryValue,String key,Object value) {
        Query query =new Query(Criteria.where(queryStr).is(queryValue));
        Update update=Update.update(key,value);
        getDatastore().updateMulti(query,update,tbName);
    }
    @Override
    public void updateAttributeSet(String tbName, String queryStr,Object queryValue,Update update) {
        Query query =new Query(Criteria.where(queryStr).is(queryValue));
        getDatastore().updateMulti(query,update,tbName);
    }
    @Override
    public void updateAttribute(String tbName, String queryStr,Object queryValue,Update update) {
        Query query =new Query(Criteria.where(queryStr).is(queryValue));
        getDatastore().updateMulti(query,update,tbName);
    }
    @Override
    public void updateAttribute(ID id,String key,Object value) {
        Query query =new Query(Criteria.where("_id").is(id));
        Update update=Update.update(key,value);
        getDatastore().updateFirst(query,update,getEntityClass());
    }
    @Override
    public void updateAttribute(ID id, String key, Object value, String collectionName) {
        Query query =new Query(Criteria.where("_id").is(id));
        Update update=Update.update(key,value);
        getDatastore().updateFirst(query,update,getEntityClass(),collectionName);
    }

    @Override
    public T queryOne(String key,Object value) {
        Query query =new Query(Criteria.where(key).is(value));
      return getDatastore().findOne(query,getEntityClass());
    }
    @Override
    public T queryOne(String key, Object value, String collectionName) {
        Query query =new Query(Criteria.where(key).is(value));
        return getDatastore().findOne(query,getEntityClass(),collectionName);
    }
    @Override
    public List<T> queryListsByQuery(Query query) {
        return getDatastore().find(query,getEntityClass());

    }
    @Override
    public List<T> queryListsByQuery(Query query, String collectionName) {
        return getDatastore().find(query,getEntityClass(),collectionName);

    }

    /**
     *
     * @param query 查询条件
     * @param sortKey 排序字段
     * @param order 1 升序  -1 降序
     * @return
     */
    @Override
    public List<T> queryListsByQuery(Query query, String sortKey, Integer order) {
        if(1==order) {
            query.with(Sort.by(Sort.Order.asc(sortKey)));
        }else {
            query.with(Sort.by(Sort.Order.desc(sortKey)));
        }
        return getDatastore().find(query,getEntityClass());
    }
    @Override
    public List<T> queryListsByQuery(Query query, String sortKey, Integer order, String collectionName) {
        if(1==order) {
            query.with(Sort.by(Sort.Order.asc(sortKey)));
        }else {
            query.with(Sort.by(Sort.Order.desc(sortKey)));
        }
        return getDatastore().find(query,getEntityClass(),collectionName);
    }
    @Override
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize) {
        if(pageSize>0) {
            query.with(PageRequest.of(pageIndex, pageSize));
        }
        return getDatastore().find(query,getEntityClass());
    }
    /**
     *
     * @param query 查询条件
     * @param pageIndex 页码
     * @param pageSize 分页size
     * @return
     */
    @Override
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, String collectionName) {
        if(pageSize>0) {
            query.with(PageRequest.of(pageIndex, pageSize));
        }
        return getDatastore().find(query,getEntityClass(),collectionName);
    }
    @Override
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, int startIndex) {
        if(1==startIndex&&pageIndex>0) {
            pageIndex -= 1;
        }
        if(pageSize>0) {
            query.with(PageRequest.of(pageIndex, pageSize));
        }
        return getDatastore().find(query,getEntityClass());
    }
    @Override
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, int startIndex, String collectionName) {
        if(1==startIndex&&pageIndex>0) {
            pageIndex -= 1;
        }
        if(pageSize>0) {
            query.with(PageRequest.of(pageIndex, pageSize));
        }
        return getDatastore().find(query,getEntityClass(),collectionName);
    }
    @Override
    public List<T> getEntityListsByKey(String key,Object value) {
        Query query =new Query(Criteria.where(key).is(value));
        return getDatastore().find(query,getEntityClass());
    }
    @Override
    public List<T> getEntityListsByKey(String key, Object value, String collectionName) {
        Query query =new Query(Criteria.where(key).is(value));
       return getDatastore().find(query,getEntityClass(),collectionName);
    }
    @Override
    public List<T> getEntityListsByQuery(Query query) {
       return getDatastore().find(query,getEntityClass());
    }
    @Override
    public List<?> getEntityListsByKey(Class<?> clazz,String key,Object value,String sort) {
        Query query =new Query(Criteria.where(key).is(value));
        if(null!=sort && !"".equals(sort)) {
            query.with(Sort.by(sort));
        }
        return getDatastore().find(query,clazz);

    }
    @Override
    public List<?> getEntityListsByKey(Class<?> clazz,String key,Object value,String sort,Integer pageIndex,Integer pageSize) {
        Query query =new Query(Criteria.where(key).is(value));
        if(null!=sort && !"".equals(sort)){
            query.with(Sort.by(sort));
        }
        if(pageSize>0) {
            query.with(PageRequest.of(pageIndex, pageSize));
        }
        return getDatastore().find(query,clazz);
    }
    //将操作保存在数据库
    @Override
    public Object saveEntity(Object entity){
        return getDatastore().save(entity);
    }
    @Override
    public Object saveEntity(Object entity,String collectionName){
        return getDatastore().save(entity,collectionName);
    }

    @Override
    public Object update(ID id,T entity){
        T dest = get(id);
        try {
            BeanUtils.copyProperties(entity, dest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return saveEntity(dest);
    }
    @Override
    public Object update(ID id,T entity,String collectionName){
        T dest = get(id);
        try {
            BeanUtils.copyProperties(entity, dest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return saveEntity(dest,collectionName);
    }
    @Override
    public Object updateEntity(Class<?> clazz,ID id,Object entity){
        Object dest = getDatastore().findById(id,clazz);
        try {
            BeanUtils.copyProperties(entity, dest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return getDatastore().save(dest);
    }
    @Override
    public Object updateEntity(Class<?> clazz,ID id,Object entity,String collectionName){
        Object dest = getDatastore().findById(id,clazz);
        try {
            BeanUtils.copyProperties(entity, dest);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return getDatastore().save(dest,collectionName);
    }
    @Override
    public T findAndDelete(String name, Query query) {
        return getDatastore().findAndRemove(query,getEntityClass());

    }
    @Override
    public T findAndDelete(String name, Query query, String collectionName) {
        return getDatastore().findAndRemove(query,getEntityClass(),collectionName);

    }
    @Override
    public T findAndModify(Query query, Update update, Class<T> entityClass){
        return getDatastore().findAndModify(query,update,new FindAndModifyOptions().returnNew(true),getEntityClass());
    }
    @Override
    public T findAndModify(Query query, Update update, Class<T> entityClass, String collectionName){
        return getDatastore().findAndModify(query,update,new FindAndModifyOptions().returnNew(true),getEntityClass(),collectionName);
    }
    @Override
    public <R> List<R> distinct(String key, Query query,Class<R> resultClass) {

        return getDatastore().findDistinct(query,key,getEntityClass(),resultClass);
    }
    @Override
    public <R> List<R> distinct(String tbName,String key, Query query,Class<R> resultClass) {

        return getDatastore().findDistinct(query,key,tbName,resultClass);
    }

    //返回一个字段的集合
    @Override
    public <R> List<R> distinct(String name,String key, DBObject q,Class<R> resultClass) {
       final Query query=new Query();

        q.keySet().forEach(k->{
            query.addCriteria(Criteria.where(k).is(q.get(k)));
        });

       return getDatastore().findDistinct(query,key,name,resultClass);
    }
    @Override
    public <R> List<R> distinct(String key, DBObject q,Class<R> resultClass) {
        final Query query=new Query();

        q.keySet().forEach(k->{
            query.addCriteria(Criteria.where(k).is(q.get(k)));
        });

        return getDatastore().findDistinct(query,key,getEntityClass(),resultClass);
    }
    @Override
    public <R> List<R> distinct(Class classz, String key, String queryKey, String queryValue, Class<R> resultClass) {
        final Query query=new Query(Criteria.where(queryKey).is(queryValue));
        return getDatastore().findDistinct(query,key,classz,resultClass);
    }


    @Override
    public Object queryOneField(String key, Document query) {
         Document projection=new Document(key, 1);

        Document dbObj = getDatastore().getCollection(getCollectionName()).find(query).projection(projection).first();
        if(null==dbObj) {
            return null;
        }

        return dbObj.get(key);
    }

    /**
     *
     * @param key 要查找的字段名
     * @param entityClass 和表对应的实体类
     * @param query  查询条件
     * @return
     */
    @Override
    public Object  queryOneField( Class<T> entityClass ,String key, Document query) {
        Document projection=new Document(key, 1);

        Document dbObj = getDatastore().getCollection(getCollectionName(entityClass)).find(query).projection(projection).first();
        if(null==dbObj) {
            return null;
        }

        return dbObj.get(key);
    }
    @Override
    public Object queryOneField( Class<T> entityClass ,String key,String queryStr,Object queryValue){
        Document query=new Document(queryStr,queryValue);
        return  queryOneField(getCollectionName(entityClass),key,query);
    }
    @Override
    public Object queryOneField(String dbName,String key, Document query) {
        Document projection=new Document(key, 1);
        Document dbObj = getDatastore().getCollection(dbName).find(query).projection(projection).first();
        if(null==dbObj) {
            return null;
        }

        return dbObj.get(key);
    }
    @Override
    public Object queryOneFieldById(String key,ID id) {
        Document query=new Document("_id", id);
        Document projection=new Document(key, 1);
        Document dbObj = getDatastore().getCollection(getCollectionName(getEntityClass())).find(query).projection(projection).first();
        if(null==dbObj) {
            return null;
        }

        return dbObj.get(key);
    }
    @Override
    public Object queryOneFieldById(String key, ID id, String collectionName) {
        Document query=new Document("_id", id);
        Document projection=new Document(key, 1);
        Document dbObj = getDatastore().getCollection(collectionName).find(query).projection(projection).first();
        if(null==dbObj) {
            return null;
        }

        return dbObj.get(key);
    }
    @Override
    public Document queryOneFields(Document query, String... keys) {
        Document projection=new Document();
        for (String str : keys) {
            projection.put(str, 1);
        }
        Document dbObj =  getDatastore().getCollection(getCollectionName(getEntityClass())).find(query).projection(projection).first();

        return dbObj;

    }
    @Override
    public Document queryOneFieldsById(ID id, String... keys) {
        Document projection=new Document();
        Document query=new Document("_id", id);
        for (String str : keys) {
            projection.put(str, 1);
        }
        Document dbObj =  getDatastore().getCollection(getCollectionName(getEntityClass())).find(query).projection(projection).first();

        return dbObj;

    }
    @Override
    public List<Document> queryListFields(Document query, String... keys) {
        Document projection=new Document();
        for (String str : keys) {
            projection.put(str, 1);
        }
        List<Document> results =new ArrayList<>();
        MongoCursor<Document> iterator = getDatastore().getCollection(getCollectionName(getEntityClass())).find(query).projection(projection).iterator();
        while (iterator.hasNext()) {
            results.add( iterator.next());
        }
        return results;

    }

    @Override
    public PageRequest createPageRequest(Integer page, Integer size) {
        if(page<0){
            page =0;
        }
        if(size<=0){
            size=10;
        }
       return PageRequest.of(page,size);
    }
    @Override
    public PageRequest createPageRequest(Integer page, Integer size, Integer startIndex) {
        if(1==startIndex&&page>0){
            page-=1;
        }
        return PageRequest.of(page,size);
    }
    @Override
    public PageRequest createPageRequest(Integer page, Integer size, Sort sort) {
        return PageRequest.of(page,size,sort);
    }
    @Override
    public PageRequest createPageRequest(Integer page, Integer size, Sort.Direction direction, String... properties) {
        return PageRequest.of(page,size,direction,properties);
    }
    @Override
    public Query descByquery(Query query, String sortKey){
        return query.with(Sort.by(Sort.Order.desc(sortKey)));
    }
    @Override
    public Query ascByquery(Query query, String sortKey){
        return query.with(Sort.by(Sort.Order.asc(sortKey)));
    }

    @Override
    public UpdateResult updateFirst(Query query, Update update) {
        return getDatastore().updateFirst(query, update,getEntityClass());
    }
    @Override
    public UpdateResult updateFirst(Query query, Update update,String collectionName) {
        return getDatastore().updateFirst(query, update,getEntityClass(),collectionName);
    }

    @Override
    public UpdateResult update(Query query, Update update) {
        return getDatastore().updateMulti(query,update,getEntityClass());
    }
    @Override
    public UpdateResult update(Query query, Update update,String collectionName) {
        return getDatastore().updateMulti(query,update,getEntityClass(),collectionName);
    }

    @Override
    public DeleteResult deleteByQuery(Query query) {
        return getDatastore().remove(query,getEntityClass());
    }
    @Override
    public DeleteResult deleteByQuery(Query query, String collectionName) {
        return getDatastore().remove(query,getEntityClass(),collectionName);
    }

    @Override
    public T get(ID id) {
        return getDatastore().findById(id,getEntityClass());
    }
    @Override
    public T get(ID id, String collectionName) {
        return getDatastore().findById(id,getEntityClass(),collectionName);
    }
    @Override
    public T queryOneById(ID id) {
        return getDatastore().findById(id,getEntityClass());
    }
    @Override
    public T queryOneById(ID id, String collectionName) {
        return getDatastore().findById(id,getEntityClass(),collectionName);
    }
    @Override
    public Object getEntityById(Class<?> clazz, ID id){
        return	getDatastore().findById(id,clazz);
    }
    @Override
    public Object getEntityById(Class<?> clazz, ID id, String collectionName){
        return	getDatastore().findById(id,clazz,collectionName);
    }

    @Override
    public boolean exists(String key, Object value) {
        Query query=new Query(Criteria.where(key).is(value));
        return getDatastore().exists(query,getEntityClass());
    }
    @Override
    public boolean exists(String key, Object value, String collectionName) {
        Query query=new Query(Criteria.where(key).is(value));
        return getDatastore().exists(query,getEntityClass(),collectionName);
    }

    @Override
    public boolean exists(Query query) {
        return getDatastore().exists(query,getEntityClass());
    }
    @Override
    public boolean exists(Query query, String collectionName) {
        return getDatastore().exists(query,getEntityClass(),collectionName);
    }

    @Override
    public Long count() {
        return getDatastore().count(new Query(),getEntityClass());
    }
    @Override
    public Long count(String collectionName) {
        return getDatastore().count(new Query(),getEntityClass(),collectionName);
    }

    @Override
    public Long count(String key, Object value) {
        Query query=new Query(Criteria.where(key).is(value));
        return getDatastore().count(query,getEntityClass());
    }
    @Override
    public Long count(String key, Object value, String collectionName) {
        Query query=new Query(Criteria.where(key).is(value));
        return getDatastore().count(query,getEntityClass(),collectionName);
    }

    @Override
    public Long count(Query query) {
        return getDatastore().count(query,getEntityClass());
    }
    @Override
    public Long count(Query query, String collectionName) {
        return getDatastore().count(query,getEntityClass(),collectionName);
    }

    @Override
    public T findOne(String key, Object value) {
        Query query=new Query(Criteria.where(key).is(value));
        return getDatastore().findOne(query,getEntityClass());
    }
    @Override
    public T findOne(String key, Object value, String collectionName) {
        Query query=new Query(Criteria.where(key).is(value));
        return getDatastore().findOne(query,getEntityClass(),collectionName);
    }

    @Override
    public <V> V findOne(Class<V> tClass, String key, Object value) {
        Query query=new Query(Criteria.where(key).is(value));
        return getDatastore().findOne(query,tClass);
    }
    @Override
    public T findOne(Query query) {
        return getDatastore().findOne(query,getEntityClass());
    }
    @Override
    public T findOne(Query query, String collectionName) {
        return getDatastore().findOne(query,getEntityClass(),collectionName);
    }
    @Override
    public void ensureIndexes() {
        getDatastore().indexOps(getEntityClass());
    }

    @Override
    public MongoCollection getCollection() {
        return getDatastore().getCollection(getCollectionName(getEntityClass()));
    }
    @Override
    public <S extends T> S save(S entity) {
        Assert.notNull(entity, "Entity must not be null!");
        return this.getDatastore().save(entity);
    }


    public void closeCursor(MongoCursor cursor){
        if(null!=cursor) {
            cursor.close();
        }
    }
}
