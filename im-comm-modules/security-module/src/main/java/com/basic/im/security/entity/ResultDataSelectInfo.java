package com.basic.im.security.entity;

import lombok.Data;

import java.util.*;

/**
 * @Description:
 * @Author xie yuan yang
 * @Date 2020/3/9
 **/
@Data
public class ResultDataSelectInfo {

    /**
     * 编号
     */
    private String id;

    /**
     * 资源名称
     */
    private String resourceName;

    /**
     * 资源路径
     */
    private String resourceUrl;

    /**
     * 资源权限
     */
    private String resourceAuth;

    /**
     * 资源类型
     * 菜单=1 按钮=0
     */
    private String type;

    /**
     * 状态
     * 正常=1  禁用=-1
     */
    private byte status;


    /**
     * 是否只展示数据
     * 1=展示 0=不展示
     */
    private int isView;

    /**
     * 子菜单
     */
    private List<ResultDataSelectInfo> children;
}
