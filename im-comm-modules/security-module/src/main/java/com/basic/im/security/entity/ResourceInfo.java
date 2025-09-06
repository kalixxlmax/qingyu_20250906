package com.basic.im.security.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * 权限.资源
 **/
@Getter
@Setter
@Document(value = "resourceInfo")
public class ResourceInfo extends BaseEntity {

    @Id
    private ObjectId id;

    /**
     * 资源名称
     */
    private String resourceName;

    /**
     *  资源路径
     */
    private String resourceUrl;

    /**
     * 资源权限
     */
    private String resourceAuth;

    /**
     * 资源类型
     * 0=按钮 1=左侧菜单 2=头部菜单
     */
    private String type;

    /**
     * 状态
     * 正常=1  禁用=-1
     */
    private byte status;

    /**
     * 上级编号
     */
    private String pid;

    /**
     * 是否只展示数据
     */
    private int isView;

    /**
     * 子菜单
     */
    private List<ResourceInfo> children;


    /*private String aa;*/
}
