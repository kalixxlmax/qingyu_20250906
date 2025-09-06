package com.basic.im.admin.entity;


import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;


/**
* 用于记录 网页 url 黑名单，白名单的表
 */
@Data
@Document(value = "webUrlRoster")
public class WebUrlRoster {

    private ObjectId _id;  // 记录id

    private String webUrl; //网页 url

    private byte urlType;  //类型值， -1 ： 黑名单   0 ： 既不是黑名单也不是白名单   1  ： 白名单

    private long createTime; //创建时间

    public WebUrlRoster() { }

    public WebUrlRoster(String webUrl, byte urlType) {
        this.webUrl = webUrl;
        this.urlType = urlType;
        this.createTime = System.currentTimeMillis();
    }
}
