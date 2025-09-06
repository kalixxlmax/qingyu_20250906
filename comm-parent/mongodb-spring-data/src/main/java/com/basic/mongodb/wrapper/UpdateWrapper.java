package com.basic.mongodb.wrapper;

import com.basic.mongodb.function.SFunction;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Map;
import java.util.function.Function;


/**
 * Update 更新包装类
 * @author chat 
 * @data   2020-9-17
 */
public class UpdateWrapper<T> extends Wrapper{

    private final Update update;

    public UpdateWrapper(){
        this(new Update());
    }

    public UpdateWrapper(Update update) {
        this.update = update;
    }

    public static <T>UpdateWrapper<T> update(SFunction<T, ?> column, Object value) {
        return new UpdateWrapper<T>(Update.update(columnToString(column),value));
    }

    public UpdateWrapper<T> set(Object obj, Function<Object,Map<String, String>> function){
        return set(function.apply(obj));
    }

    public UpdateWrapper<T> set(Map<String,String> map){
        map.forEach(update::set);
        return this;
    }

    public UpdateWrapper<T> set(SFunction<T, ?> column, Object value) {
        update.set(columnToString(column),value);
        return this;
    }

    public UpdateWrapper<T> setOnInsert(SFunction<T, ?> column, Object value) {
        update.setOnInsert(columnToString(column),value);
        return this;
    }

    public UpdateWrapper<T> unset(SFunction<T, ?> column) {
        update.unset(columnToString(column));
        return this;
    }

    public UpdateWrapper<T> push(SFunction<T, ?> column,Object value) {
        update.push(columnToString(column),value);
        return this;
    }

    public UpdateWrapper<T> pull(SFunction<T, ?> column,Object value) {
        update.pull(columnToString(column),value);
        return this;
    }

    public UpdateWrapper<T> pullAll(SFunction<T, ?> column,Object[] value) {
        update.pullAll(columnToString(column),value);
        return this;
    }

    public UpdateWrapper<T> addToSet(SFunction<T, ?> column) {
        update.addToSet(columnToString(column));
        return this;
    }

    public UpdateWrapper<T> addToSet(SFunction<T, ?> column,Object value) {
        update.addToSet(columnToString(column),value);
        return this;
    }

    public UpdateWrapper<T> pop(SFunction<T, ?> column, Update.Position pos) {
        update.pop(columnToString(column),pos);
        return this;
    }

    public UpdateWrapper<T> rename(SFunction<T, ?> column, String newName) {
        update.rename(columnToString(column),newName);
        return this;
    }

    public UpdateWrapper<T> currentDate(SFunction<T, ?> column) {
        update.currentDate(columnToString(column));
        return this;
    }

    public UpdateWrapper<T> currentTimestamp(SFunction<T, ?> column) {
        update.currentTimestamp(columnToString(column));
        return this;
    }

    public UpdateWrapper<T> inc(SFunction<T, ?> column) {
        return this.inc(column,1L);
    }

    public UpdateWrapper<T> inc(SFunction<T, ?> column,Number inc) {
        update.inc(columnToString(column),inc);
        return this;
    }

    public UpdateWrapper<T> multiply(SFunction<T, ?> column,Number multiplier) {
        update.multiply(columnToString(column),multiplier);
        return this;
    }

    public UpdateWrapper<T> max(SFunction<T, ?> column,Object value) {
        update.max(columnToString(column),value);
        return this;
    }

    public UpdateWrapper<T> min(SFunction<T, ?> column,Object value) {
        update.min(columnToString(column),value);
        return this;
    }

    /**
     * 返回Update实体类对象
     */
    public Update build(){
        return update;
    }

}
