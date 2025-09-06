package com.basic.im.admin.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @Description: TODO (密保问题)
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Data
@Document("secret_question")
public class SecretQuestion {
    private ObjectId id;

    /**
     * 密保问题
     **/
    private String question;

    /**
     * 状态
     * -1  禁用
     *  1  使用
     **/
    private byte status;

    /**
     * 创建时间
     **/
    private Long createTime;
}
