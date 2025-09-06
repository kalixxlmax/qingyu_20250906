package com.basic.im.sms.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * 手机短信验证码日志表
 * 检验时，生成一条数据
 * 发送时，生成一条数据
 */
@Data
@Accessors(chain = true)
@Document
public class SmsSendLog {
    private @Id  ObjectId id;
    @Indexed
    private String phone;      // 手机号码
    @Indexed
    private String ip;         // 调用者IP
    private Long   createTime; // 创建日期
    private Byte   isSend;     // 是否发送了验证码
}
