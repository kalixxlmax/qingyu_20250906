package com.basic.im.security.dto;

import com.basic.im.security.entity.BaseEntity;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @Description: TODO (资源)
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
@Getter
@Setter
public class ResourceInfoDTO extends BaseEntity {
    @Id
    private String id;
    //资源名称
    private String resourceName;
    //资源路径
    private String resourceUrl;
    //资源权限
    private String resourceAuth;
    //资源类型
    private String type; //菜单=1 按钮=0
    //状态
    private byte status;//正常=1  禁用=-1
    //上级编号  0表示没有上级
    private String pid = "0";
    //是否只展示数据  1=展示 0=不展示
    private int isView;
}
