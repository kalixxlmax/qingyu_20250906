package com.basic.mongodb.morphia;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.basic.common.core.Callback;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.dao.DAO;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import java.util.List;

public interface IMorphiaDAO<T,ID> extends DAO<T,ID> {

    Morphia getMorphia();

    Class<T> getEntityClass();

    MongoClient getMongoClient();

    Datastore getDatastore();

    String getCollectionName(long userId);

    MongoCollection getMongoCollection(String dbName);

    MongoCollection getCollection(long userId);

    MongoCollection<DBObject> getDBObjectCollection(int userId);

    String getCollectionName(int userId, int remainder);

    String getCollectionName();

    List<String> getCollectionList();

    DBObject objectToDBObject(Object entity);

    void updateAttribute(ID id, T entity);

    void updateAttributeByIdAndKey(ID id, String key, Object value);

    void updateAttributeByIdAndKey(Class<?> clazz, ID id, String key, Object value);

    boolean updateAttributeByOps(ID id, UpdateOperations<T> ops);
    boolean updateAttribute(String queryStr, Object queryValue, String key, Object value);

    void updateAttribute(String tbName, String queryStr, Object queryValue, String key, Object value);

    List<T> getEntityListsByKey(String key, Object value);

    void updateAttributeSet(String tbName, String queryStr, Object queryValue, BasicDBObject value);

    void updateAttribute(String tbName, String queryStr, Object queryValue, BasicDBObject update);

    void updateAttribute(ID id, String key, Object value);

    T queryOne(String key, Object value);


    List<?> getEntityListsByKey(Class<?> clazz, String key, Object value, String sort);

    List<?> getEntityListsByKey(Class<?> clazz, String key, Object value, String sort, int pageIndex, int pageSize);
    Object saveEntity(Object entity);
    Object update(ID id, T entity);
    Object updateEntity(Class<?> clazz, ID id, Object entity);
    List<Object> findAndDelete(String name, DBObject q);
    List distinct(String name, String key, DBObject q);
    List distinct(String key, DBObject q);

    Object queryOneField(String key, DBObject query);

    Object queryOneField(Class<T> entityClass, String key, DBObject query);

    Object queryOneField(Class<T> entityClass, String key, String queryStr, Object queryValue);

    Object queryOneField(String dbName, String key, DBObject query);

    Object queryOneFieldById(String key, ID id);

    BasicDBObject findAndModify(String name, DBObject query, DBObject update);

    List<Object> findAndUpdate(String name, DBObject q, DBObject ops, DBObject keys, Callback callback);

    public <T> DBCollection getCollection(Query<T> q);

    List<Object> selectId(String name, QueryBuilder qb);

    List<?> keysToIds(final List<Key<T>> keys);

    Query<T> createQuery();

    UpdateOperations<T> createUpdateOperations();

    FindOptions pageFindOption(int page, int limit, int start);


    <V> V findOne(Class<V> tClass, String key, Object value);
}
