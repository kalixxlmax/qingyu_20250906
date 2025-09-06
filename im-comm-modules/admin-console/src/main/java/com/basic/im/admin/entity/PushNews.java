package com.basic.im.admin.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @ClassName PushNews
 * @Author xie yuan yang
 * @date 2020.11.09 15:52
 * @Description 推送消息实体
 */
@Data
@Document("pushNews")
public class PushNews {
    @Id
    private ObjectId id;

    /**
     * 推送标题
     **/
    private String title;

    /**
     * 内容
     **/
    private String content;

    /**
     * 推送机型
     **/
    private int type;

    /**
     * 推送 包名
     **/
    private String packageName;

    /**
     * 推送 token
     **/
    private String token;

    /**
     * URL地址
     * 全员通知才需要 URL地址
     **/
    private String addressURL;

    /**
     * 发送时间
     **/
    private long createTime;


    public interface Type{
        int ALL_PUSH        = 0;    // 全员 通知
        int APNS_PUSH       = 1;    // apns 推送
        int APNS_VOIP_PUSH  = 2;
        int BAIDU_PUSH      = 3;    // 百度 推送
        int XIAOMI_PUSH     = 4;    // 小米 推送
        int HUAWEI_PUSH     = 5;    // 华为 推送
        int JPUSH_PUSH      = 6;    // 极光 推送
        int FCM_PUSH        = 7;    // Google 推送
        int MEIZU_PUSH      = 8;    // 魅族 推送
        int VIVO_PUSH       = 9;    // VIVO 推送
        int OPPO_PUSH       = 10;   // OPPO 推送
    }


    public PushNews() {
    }

    public PushNews(String title, String content, int type, String packageName, String token, long createTime) {
        this.title = title;
        this.content = content;
        this.type = type;
        this.packageName = packageName;
        this.token = token;
        this.createTime = createTime;
    }

    public PushNews( String title, String content, String addressURL, int type, long createTime) {
        this.title = title;
        this.content = content;
        this.addressURL = addressURL;
        this.createTime = createTime;
        this.type = type;
    }
}
