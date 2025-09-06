package com.basic.mongodb.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;


/**
 * 字段信息缓存
 * @author chat 
 * @date   2020-9-17
 */
@Data
@AllArgsConstructor
public class ColumnCache implements Serializable {

    private static final long serialVersionUID = -4586291538088403456L;


    /**
     * 属性名称
     */
    private String column;
    /**
     * 自定义的字段名称
     */
    private String columnField;

    public static ColumnCache instance(String column, String columnField){
        return new ColumnCache(column, columnField);
    }
}