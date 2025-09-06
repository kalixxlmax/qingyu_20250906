package com.basic.im.admin.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @Description: TODO （发现页实体类）
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Data
@Document("disCoverApp")
public class DisCoverApp {

    private ObjectId id;

    //名称
    private String name;

    //图标
    private String imgUrl;

    //链接地址
    private String linkAddres;

    //顺序
    private int sequence;

    //是否展示 1展示 -1不展示
    private byte isShow;

    //创建时间
    private Long createTime;

    //更新时间
    private Long modirTime;
}
