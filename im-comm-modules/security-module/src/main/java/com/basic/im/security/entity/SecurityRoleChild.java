package com.basic.im.security.entity;

import lombok.Data;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @Description:
 * @Author xie yuan yang
 * @Date 2020/3/18
 **/
@Data
public class SecurityRoleChild {

    /**
     * 角色编号
     */
    private String roleId;

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

}
