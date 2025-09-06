package com.basic.im.utils;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MongoUtil {

    public final static String DIAGONAL              =  "\\";
    public final static String DOUBT                 =  "?";
    public final static String ALL                   =  "*";
    public final static String ADD                   =  "+";
    public final static String LEFT_BRACKET          =  "(";
    public final static String RIGHT_BRACKET         =  ")";
    public final static String LEFT_SQUARE_BRACKET   =  "[";
    public final static String RIGHT_SQUARE_BRACKET  =  "]";

    public static List<String> NOT_QUERY_LIST;

    static {
        NOT_QUERY_LIST = new ArrayList<>();
        NOT_QUERY_LIST.add(DOUBT);
        NOT_QUERY_LIST.add(ALL);
        NOT_QUERY_LIST.add(ADD);
        NOT_QUERY_LIST.add("^");
        NOT_QUERY_LIST.add("$");
        NOT_QUERY_LIST.add(LEFT_BRACKET);
        NOT_QUERY_LIST.add(RIGHT_BRACKET);
        NOT_QUERY_LIST.add(LEFT_SQUARE_BRACKET);
        NOT_QUERY_LIST.add(RIGHT_SQUARE_BRACKET);
    }

    /**
     * 设置更新
     */
    public static <T>void update(Update update, String key, T value){
        if (StrUtil.isNotBlank(key) && ObjectUtil.isNotEmpty(value)){
            if (ObjectUtil.isEmpty(update)){
                Update.update(key,value);
            }else{
                update.set(key,value);
            }
        }
    }

    /**
     * 设置查询条件
     */
    public static <T> void query(Query query, String key, T value){
        if (StrUtil.isNotBlank(key) && ObjectUtil.isNotEmpty(value)){
            if (query == null){
                Query.query(Criteria.where(key).is(value));
            }else{
                query.addCriteria(Criteria.where(key).is(value));
            }
        }
    }

    /**
     * 聚合获取指定的统计结果
     * @param criteria 查询条件
     * @param groupKey 分组 key
     * @param key      统计字段
     * @return 统计结果
     */
    public static <T>T sumAggregation(Criteria criteria, String groupKey, String key, MongoTemplate mongoTemplate, Class<?> inputType, T outType){
        Aggregation aggregation = Aggregation.newAggregation(Aggregation.match(criteria),Aggregation.group(groupKey).sum(key).as(key));
        AggregationResults<Document> aggregate = mongoTemplate.aggregate(aggregation, inputType, Document.class);
        Iterator<Document> iterator = aggregate.iterator();
        if (outType instanceof Long){
            return (T) sumLong(iterator,key);
        }
        if (outType instanceof Integer){
            return (T) sumInteger(iterator,key);
        }
        if (outType instanceof Double){
            return (T) sumDouble(iterator,key);
        }
        return outType;
    }

    private static Integer sumInteger(Iterator<Document> iterator,String key){
        Document document;
        Integer result = 0;
        while (iterator.hasNext()){
            document = iterator.next();
            result+=document.getInteger(key);
        }
        return result;
    }

    private static Long sumLong(Iterator<Document> iterator,String key){
        Document document;
        Long result = 0L;
        while (iterator.hasNext()){
            document = iterator.next();
            result+=document.getLong(key);
        }
        return result;
    }

    private static Double sumDouble(Iterator<Document> iterator,String key){
        Document document;
        Double result = 0.0;
        while (iterator.hasNext()){
            document = iterator.next();
            result+=document.getDouble(key);
        }
        return result;
    }

    /**
     * 转换模糊查询无法处理的英文字符
     */
    public static String tranKeyWord(String keyWord){
        if (StrUtil.isBlank(keyWord)){
            return StrUtil.EMPTY;
        }

        if (keyWord.contains(DIAGONAL)){
            keyWord = keyWord.replace(DIAGONAL,"");
        }

        for (String contStr : NOT_QUERY_LIST) {
            if (keyWord.contains(contStr)){
                System.out.println(StrUtil.BACKSLASH + contStr);
                System.out.println(StrUtil.BACKSLASH + StrUtil.BACKSLASH + contStr);
                keyWord = keyWord.replaceAll(StrUtil.BACKSLASH + contStr,StrUtil.BACKSLASH + StrUtil.BACKSLASH + contStr);
            }
        }
        return keyWord;
    }
}