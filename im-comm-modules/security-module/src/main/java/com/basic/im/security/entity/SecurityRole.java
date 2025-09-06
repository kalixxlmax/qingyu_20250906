package com.basic.im.security.entity;

import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.*;

/**
 * @Description: 角色
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
@Getter
@Setter
@Document(value = "security_role")
public class SecurityRole extends BaseEntity {

    /**
     * 角色编号
     */
    @Id
    private ObjectId roleId;

    /**
     * 角色名称
     */
    private String roleName;

    /**
     * 角色描述
     */
    private String roleDesc;

    /**
     * 角色状态
     * 正常=1 禁用=-1
     */
    private String status;

    /**
     * 角色权限
     */
    private List<String> roleResourceList;

    /**
     * 兼容旧版本权限字段
     * 0=普通用户  1=游客（用于后台浏览数据）；2=公众号 ；3=机器账号，由系统自动生成；4=客服账号;5=管理员；6=超级管理员；7=财务;8=其他；
     */
    private byte role;

    /*private String roleId1;*/
}
