package com.basic.mongodb.dynamic;

import com.mongodb.DBObject;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.basic.mongodb.springdata.BaseMongoRepository;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import java.io.Serializable;
import java.util.List;


public abstract class DynamicMongoRepository<T, ID extends Serializable> extends BaseMongoRepository<T, ID> {

    @Autowired
    protected DynamicMongoTemplate dynamicMongoTemplate;

    @Override
    public MongoTemplate getDatastore() {
        return dynamicMongoTemplate;
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.DELETE)
    public DeleteResult deleteById(ID id) {
        return super.deleteById(id);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.DELETE)
    public DeleteResult deleteById(ID id, String collectionName) {
        return super.deleteById(id, collectionName);
    }


    @Override
    @TargetDataSource(operator = OperatorMethod.DELETE)
    public DeleteResult deleteByAttribute(String key, Object value) {
        return super.deleteByAttribute(key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.DELETE)
    public DeleteResult deleteByAttribute(String key, Object value, String collectionName) {
        return super.deleteByAttribute(key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttribute(ID id, T entity) {
        super.updateAttribute(id, entity);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttribute(ID id, T entity, String collectionName) {
        super.updateAttribute(id, entity, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttributeByIdAndKey(ID id, String key, Object value) {
        super.updateAttributeByIdAndKey(id, key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttributeByIdAndKey(ID id, String key, Object value, String collectionName) {
        super.updateAttributeByIdAndKey(id, key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttributeByIdAndKey(Class<?> clazz, ID id, String key, Object value) {
        super.updateAttributeByIdAndKey(clazz, id, key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttributeByIdAndKey(Class<?> clazz, ID id, String key, Object value, String collectionName) {
        super.updateAttributeByIdAndKey(clazz, id, key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public boolean updateAttributeByOps(ID id, Update update) {
        return super.updateAttributeByOps(id, update);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public boolean updateAttributeByOps(ID id, Update update, String collectionName) {
        return super.updateAttributeByOps(id, update, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public boolean updateAttribute(String queryStr, Object queryValue, String key, Object value) {
        return super.updateAttribute(queryStr, queryValue, key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttribute(String tbName, String queryStr, Object queryValue, String key, Object value) {
        super.updateAttribute(tbName, queryStr, queryValue, key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttributeSet(String tbName, String queryStr, Object queryValue, Update update) {
        super.updateAttributeSet(tbName, queryStr, queryValue, update);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttribute(String tbName, String queryStr, Object queryValue, Update update) {
        Query query = new Query(Criteria.where(queryStr).is(queryValue));
        getDatastore().updateMulti(query, update, tbName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttribute(ID id, String key, Object value) {
        super.updateAttribute(id, key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public void updateAttribute(ID id, String key, Object value, String collectionName) {
        super.updateAttribute(id, key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T queryOne(String key, Object value) {
        return super.queryOne(key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T queryOne(String key, Object value, String collectionName) {
        return super.queryOne(key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query) {
        return super.queryListsByQuery(query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query, String collectionName) {
        return super.queryListsByQuery(query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query, String sortKey, Integer order) {
        return super.queryListsByQuery(query, sortKey, order);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query, String sortKey, Integer order, String collectionName) {
        return super.queryListsByQuery(query, sortKey, order, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize) {
        return super.queryListsByQuery(query, pageIndex, pageSize);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, String collectionName) {
        return super.queryListsByQuery(query, pageIndex, pageSize, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, int startIndex) {
        return super.queryListsByQuery(query, pageIndex, pageSize, startIndex);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> queryListsByQuery(Query query, Integer pageIndex, Integer pageSize, int startIndex, String collectionName) {
        return super.queryListsByQuery(query, pageIndex, pageSize, startIndex, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> getEntityListsByKey(String key, Object value) {
        return super.getEntityListsByKey(key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> getEntityListsByKey(String key, Object value, String collectionName) {
        return super.getEntityListsByKey(key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<T> getEntityListsByQuery(Query query) {
        return super.getEntityListsByQuery(query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<?> getEntityListsByKey(Class<?> clazz, String key, Object value, String sort) {
        return super.getEntityListsByKey(clazz, key, value, sort);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<?> getEntityListsByKey(Class<?> clazz, String key, Object value, String sort, Integer pageIndex, Integer pageSize) {
        return super.getEntityListsByKey(clazz, key, value, sort, pageIndex, pageSize);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.INSERT)
    public Object saveEntity(Object entity) {
        return super.saveEntity(entity);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.INSERT)
    public Object saveEntity(Object entity, String collectionName) {
        return super.saveEntity(entity, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public Object update(ID id, T entity) {
        return super.update(id, entity);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public Object update(ID id, T entity, String collectionName) {
        return super.update(id, entity, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public Object updateEntity(Class<?> clazz, ID id, Object entity) {
        return super.updateEntity(clazz, id, entity);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public Object updateEntity(Class<?> clazz, ID id, Object entity, String collectionName) {
        return super.updateEntity(clazz, id, entity, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public <R> List<R> distinct(String key, Query query, Class<R> resultClass) {
        return super.distinct(key, query, resultClass);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public <R> List<R> distinct(String tbName, String key, Query query, Class<R> resultClass) {
        return super.distinct(tbName, key, query, resultClass);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public <R> List<R> distinct(String name, String key, DBObject q, Class<R> resultClass) {
        return super.distinct(name, key, q, resultClass);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public <R> List<R> distinct(String key, DBObject q, Class<R> resultClass) {
        return super.distinct(key, q, resultClass);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public <R> List<R> distinct(Class classz, String key, String queryKey, String queryValue, Class<R> resultClass) {
        return distinct(classz, key, queryKey, queryValue, resultClass);
    }


    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object queryOneField(String key, Document query) {
       return super.queryOneField(key, query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object queryOneField(Class<T> entityClass, String key, Document query) {
        return super.queryOneField(entityClass, key, query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object queryOneField(Class<T> entityClass, String key, String queryStr, Object queryValue) {
        return super.queryOneField(entityClass, key, queryStr, queryValue);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object queryOneField(String dbName, String key, Document query) {
        return super.queryOneField(dbName, key, query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object queryOneFieldById(String key, ID id) {
        return super.queryOneFieldById(key, id);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object queryOneFieldById(String key, ID id, String collectionName) {
        return super.queryOneFieldById(key, id, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Document queryOneFields(Document query, String... keys) {
        return super.queryOneFields(query,keys);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Document queryOneFieldsById(ID id, String... keys) {
        return super.queryOneFieldsById(id, keys);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public List<Document> queryListFields(Document query, String... keys) {
        return super.queryListFields(query, keys);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public UpdateResult updateFirst(Query query, Update update) {
        return super.updateFirst(query, update);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public UpdateResult updateFirst(Query query, Update update, String collectionName) {
        return super.updateFirst(query, update, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public UpdateResult update(Query query, Update update) {
        return super.update(query, update);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.UPDATE)
    public UpdateResult update(Query query, Update update, String collectionName) {
        return super.update(query, update, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.DELETE)
    public DeleteResult deleteByQuery(Query query) {
        return super.deleteByQuery(query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.DELETE)
    public DeleteResult deleteByQuery(Query query, String collectionName) {
        return super.deleteByQuery(query, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T get(ID id) {
        return super.get(id);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T get(ID id, String collectionName) {
        return super.get(id,collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T queryOneById(ID id) {
        return super.queryOneById(id);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T queryOneById(ID id, String collectionName) {
        return super.queryOneById(id,collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object getEntityById(Class<?> clazz, ID id) {
        return super.getEntityById(clazz, id);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public Object getEntityById(Class<?> clazz, ID id, String collectionName) {
        return getEntityById(clazz, id, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.EXISTS)
    public boolean exists(String key, Object value) {
        return super.exists(key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.EXISTS)
    public boolean exists(String key, Object value, String collectionName) {
        return super.exists(key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.EXISTS)
    public boolean exists(Query query) {
        return super.exists(query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.EXISTS)
    public boolean exists(Query query, String collectionName) {
        return super.exists(query, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.COUNT)
    public Long count() {
        return super.count();
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.COUNT)
    public Long count(String collectionName) {
        return super.count(collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.COUNT)
    public Long count(String key, Object value) {
        return super.count(key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.COUNT)
    public Long count(String key, Object value, String collectionName) {
        return super.count(key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.COUNT)
    public Long count(Query query) {
        return super.count(query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.COUNT)
    public Long count(Query query, String collectionName) {
        return super.count(query, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T findOne(String key, Object value) {
        return super.findOne(key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T findOne(String key, Object value, String collectionName) {
        return super.findOne(key, value, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public <V> V findOne(Class<V> tClass, String key, Object value) {
        return super.findOne(tClass, key, value);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T findOne(Query query) {
        return super.findOne(query);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.SELECT)
    public T findOne(Query query, String collectionName) {
        return super.findOne(query, collectionName);
    }

    @Override
    @TargetDataSource(operator = OperatorMethod.INSERT)
    public <S extends T> S save(S entity) {
        return super.save(entity);
    }
}
