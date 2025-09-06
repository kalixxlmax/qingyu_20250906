package com.basic.mongodb.wrapper;


import com.basic.mongodb.function.SFunction;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Query 查询类包装
 * @author chat 
 * @data   2020-9-17
 */
public class QueryWrapper<T> extends Wrapper {

    private Criteria criteria;
    private List<CriteriaDefinition> criterias;

    public QueryWrapper() {
    }

    public QueryWrapper(Criteria criteria) {
        this.criteria = criteria;
    }

    public static <T> QueryWrapper<T> query(SFunction<T, ?> column, Object value) {
        return new QueryWrapper<T>(Criteria.where(columnToString(column)).is(value));
    }

    public QueryWrapper<T> eq(SFunction<T, ?> column, Object value) {
        criteria = dispose(column).is(value);
        return this;
    }

    public QueryWrapper<T> ne(SFunction<T, ?> column, Object value) {
        criteria = dispose(column).ne(value);
        return this;
    }

    /**
     * LESS THAN 小于
     */
    public QueryWrapper<T> lt(SFunction<T, ?> column, Object value) {
        criteria = dispose(column).lt(value);
        return this;
    }

    /**
     * LESS THAN OR EQUAL 小于等于
     */
    public QueryWrapper<T> lte(SFunction<T, ?> column, Object value) {
        criteria = dispose(column).lte(value);
        return this;
    }

    /**
     * GREATER THAN 大于
     */
    public QueryWrapper<T> gt(SFunction<T, ?> column, Object value) {
        criteria = dispose(column).gt(value);
        return this;
    }

    /**
     * GREATER THAN OR EQUAL 大于等于
     */
    public QueryWrapper<T> gte(SFunction<T, ?> column, Object value) {
        criteria = dispose(column).gte(value);
        return this;
    }

    public QueryWrapper<T> in(SFunction<T, ?> column, Object... o) {
        return in(column, Arrays.asList(o));
    }

    public QueryWrapper<T> in(SFunction<T, ?> column, Collection<?> c) {
        criteria = dispose(column).in(c);
        return this;
    }

    public QueryWrapper<T> nin(SFunction<T, ?> column, Object... o) {
        return nin(column, Arrays.asList(o));
    }

    public QueryWrapper<T> nin(SFunction<T, ?> column, Collection<?> c) {
        criteria = dispose(column).nin(c);
        return this;
    }

    public QueryWrapper<T> nin(SFunction<T, ?> column, Number value, Number remainder) {
        criteria =  dispose(column).mod(value, remainder);
        return this;
    }

    public QueryWrapper<T> all(SFunction<T, ?> column, Object... o) {
        criteria = dispose(column).all(Arrays.asList(o));
        return this;
    }

    public QueryWrapper<T> all(SFunction<T, ?> column, Collection<?> c) {
        criteria = dispose(column).all(c);
        return this;
    }

    public QueryWrapper<T> size(SFunction<T, ?> column, int size) {
        criteria = dispose(column).size(size);
        return this;
    }

    public QueryWrapper<T> exists(SFunction<T, ?> column, boolean isExists) {
        criteria = dispose(column).exists(isExists);
        return this;
    }

    public QueryWrapper<T> type(SFunction<T, ?> column, int type) {
        criteria = dispose(column).type(type);
        return this;
    }

    public QueryWrapper<T> not(SFunction<T, ?> column) {
        criteria = dispose(column).not();
        return this;
    }

    public QueryWrapper<T> regex(SFunction<T, ?> column, String re) {
        criteria = dispose(column).regex(re, null);
        return this;
    }

    public QueryWrapper<T> regex(SFunction<T, ?> column, String re, String options) {
        criteria = dispose(column).regex(re, options);
        return this;
    }

    /**
     * 处理字段名称
     */
    private Criteria dispose(SFunction<T, ?> column) {
        if (criteria != null) {
            return criteria.and(columnToString(column));
        } else {
            return Criteria.where(columnToString(column));
        }
    }

    /**
     * 添加查询条件
     */
    public QueryWrapper<T> addCriteria(CriteriaDefinition criteriaDefinition) {
        if (this.criterias == null) {
            criterias = new ArrayList<>();
        }
        this.criterias.add(criteriaDefinition);
        return this;
    }

    /**
     * 返回 Query 查询类对象
     */
    public Query build() {
        Query query = new Query();
        if (criteria != null) {
            query = Query.query(criteria);
        }
        if (criterias != null) {
            criterias.forEach(query::addCriteria);
        }
        return query;
    }
}
