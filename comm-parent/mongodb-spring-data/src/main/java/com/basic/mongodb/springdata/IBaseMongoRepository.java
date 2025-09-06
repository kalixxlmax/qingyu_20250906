package com.basic.mongodb.springdata;

import com.mongodb.ClientSessionOptions;
import com.mongodb.DBObject;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.repository.NoRepositoryBean;

import java.io.Serializable;
import java.util.List;

@NoRepositoryBean
public interface IBaseMongoRepository<T, ID extends Serializable>  {


    MongoTemplate getDatastore();

    MongoCollection getMongoCollection(String dbName);

    MongoCollection getCollection(long userId);

    MongoCollection<DBObject> getDBObjectCollection(int userId);

    String getCollectionName(int userId, int remainder);

    String getCollectionName();

    List<String> getCollectionList();


    ClientSession startSession(ClientSessionOptions sessionOptions);

    MongoTemplate withSession(ClientSession clientSession);



    /**
     * 开启事务
     */
    ClientSession startTransaction();

    /**
     * 开启事务
     */
    ClientSession startTransaction(ClientSession clientSession);

    Document objectToDocument(Object entity);

    ClientSession startSession();

    Query createQuery();

    Query createQuery(ID id);

    Query createQuery(String key, Object value);

    Query addToQuery(Query query, String key, Object value);

    Update createUpdate();

    Criteria createCriteria();

    Criteria createCriteria(String key, Object value);

    Criteria contains(String key, String value);

    Criteria containsIgnoreCase(String key, String value);

    DeleteResult deleteById(ID id);

    DeleteResult deleteById(ID id, String collectionName);

    DeleteResult deleteByAttribute(String key, Object value);

    DeleteResult deleteByAttribute(String key, Object value, String collectionName);

    void updateAttribute(ID id, T entity);

    void updateAttribute(ID id, T entity, String collectionName);

    void updateAttributeByIdAndKey(ID id, String key, Object value);

    void updateAttributeByIdAndKey(ID id, String key, Object value, String collectionName);

    void updateAttributeByIdAndKey(Class<?> clazz, ID id, String key, Object value);

    void updateAttributeByIdAndKey(Class<?> clazz, ID id, String key, Object value, String collectionName);

    boolean updateAttributeByOps(ID id, Update ops);

    boolean updateAttributeByOps(ID id, Update update, String collectionName);

    boolean updateAttribute(String queryStr, Object queryValue, String key, Object value);

    void updateAttribute(String tbName, String queryStr, Object queryValue, String key, Object value);

    T queryOne(String key, Object value, String collectionName);

    List<T> queryListsByQuery(Query query);

    List<T> queryListsByQuery(Query query, String collectionName);

    List<T> queryListsByQuery(Query query, String sortKey, Integer order);

    List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize);

    List<T> queryListsByQuery(Query query, String sortKey, Integer order, String collectionName);

    List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, String collectionName);

    List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, int startIndex);

    List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, int startIndex, String collectionName);

    List<T> getEntityListsByKey(String key, Object value);

    void updateAttributeSet(String tbName, String queryStr, Object queryValue, Update value);

    void updateAttribute(String tbName, String queryStr, Object queryValue, Update update);

    void updateAttribute(ID id, String key, Object value);

    void updateAttribute(ID id, String key, Object value, String collectionName);

    T queryOne(String key, Object value);


    List<T> getEntityListsByKey(String key, Object value, String collectionName);

    List<T> getEntityListsByQuery(Query query);

    List<?> getEntityListsByKey(Class<?> clazz, String key, Object value, String sort);

    List<?> getEntityListsByKey(Class<?> clazz, String key, Object value, String sort, Integer pageIndex, Integer pageSize);
    Object saveEntity(Object entity);

    Object saveEntity(Object entity, String collectionName);

    Object update(ID id, T entity);

    Object update(ID id, T entity, String collectionName);

    Object updateEntity(Class<?> clazz, ID id, Object entity);

    Object updateEntity(Class<?> clazz, ID id, Object entity, String collectionName);

    T findAndDelete(String name, Query q);

    <R> List<R> distinct(String key, DBObject q, Class<R> resultClass);
    <R> List<R> distinct(String tbName, String key, DBObject q, Class<R> resultClass);

    T findAndDelete(String name, Query query, String collectionName);

    T findAndModify(Query query, Update update, Class<T> entityClass);

    T findAndModify(Query query, Update update, Class<T> entityClass, String collectionName);

    <R> List<R> distinct(String key, Query query, Class<R> resultClass);

    <R> List<R> distinct(String tbName, String key, Query query, Class<R> resultClass);

    <R> List<R> distinct(Class classz, String key, String queryKey, String queryValue, Class<R> resultClass);

    Object queryOneField(String key, Document query);

    Object queryOneField(Class<T> entityClass, String key, Document query);

    Object queryOneField(Class<T> entityClass, String key, String queryStr, Object queryValue);

    Object queryOneField(String dbName, String key, Document query);

    Object queryOneFieldById(String key, ID id);


    Object queryOneFieldById(String key, ID id, String collectionName);

    Document queryOneFields(Document query, String... keys);

    Document queryOneFieldsById(ID id, String... keys);

    List<Document> queryListFields(Document query, String... keys);

    PageRequest createPageRequest(Integer page, Integer size);

    PageRequest createPageRequest(Integer page, Integer size, Integer startIndex);

    PageRequest createPageRequest(Integer page, Integer size, Sort sort);

    PageRequest createPageRequest(Integer page, Integer size, Sort.Direction direction, String... properties);

    Query descByquery(Query query, String sortKey);

    Query ascByquery(Query query, String sortKey);

    UpdateResult updateFirst(Query query, Update update);

    UpdateResult updateFirst(Query query, Update update, String collectionName);

    UpdateResult update(Query query, Update update);

    UpdateResult update(Query query, Update update, String collectionName);

    DeleteResult deleteByQuery(Query query);

    DeleteResult deleteByQuery(Query query, String collectionName);

    T get(ID id);

    T get(ID id, String collectionName);

    T queryOneById(ID id);

    T queryOneById(ID id, String collectionName);

    Object getEntityById(Class<?> clazz, ID id);

    Object getEntityById(Class<?> clazz, ID id, String collectionName);

    boolean exists(String key, Object value);

    boolean exists(String key, Object value, String collectionName);

    boolean exists(Query query);

    boolean exists(Query query, String collectionName);

    Long count();

    Long count(String collectionName);

    Long count(String key, Object value);

    Long count(String key, Object value, String collectionName);

    Long count(Query query);

    Long count(Query query, String collectionName);

    T findOne(String key, Object value);

    T findOne(String key, Object value, String collectionName);

    <V> V findOne(Class<V> tClass, String key, Object value);

    T findOne(Query query);

    T findOne(Query query, String collectionName);

    void ensureIndexes();

    MongoCollection getCollection();

    <S extends T> S save(S entity);
}
