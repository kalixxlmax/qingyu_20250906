package com.basic.im.security.entity;

import lombok.Data;

/**
 * @Description: 用户多选下拉框 自定义返回格式
 * @Author xie yuan yang
 * @Date 2020/3/6
 **/
@Data
public class ResultDataSelect {

    /**
     * 名称
     */
    private String name;

    /**
     * 数值
     */
    private String value;

    /**
     * 是否展开
     */
    private String selected = "";

    /**
     * 是否隐藏
     */
    private String disabled = "";
}
