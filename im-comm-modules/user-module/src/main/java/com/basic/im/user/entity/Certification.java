package com.basic.im.user.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author
 * @version V1.0
 * @Description: 用户相关认证信息
 * @date 2020/3/23 11:34
 */
@Data
@Document(value = "certification")
public class Certification {
    @Id
    private String id;

    private Integer userId;

    private int type;// 认证类型，1：身份实名认证

    private String certifiedAccount;// 认证信息账号

    private String certifiedName;// 认证信息用户名称

    private String certifiedAddress;// 认证信息地址

    private String backImageUrl;// 认证图片背面

    private String frontImageUrl;// 认证图片正面

    private String faceImageUrl;// 人像正面照图片

    private long startDate; // 证件有效期开始时间 格式为：yyyy-MM-dd。

    private long endDate; // 证件有效期结束时间 格式为：yyyy-MM-dd。




}
