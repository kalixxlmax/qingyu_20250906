package com.basic.im.invite.entity;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

/**
 * @author zhm
 * @version V1.0
 * @date 2019/11/14 16:06
 */
@ApiModel("邀请")
@Data
@Document(value = "invite")
public class Invite {
    @Id
    private ObjectId id;

    private int userId; // 自己的UserId

    private List<Grade> gradeList;// 上级关系列表

    private long createTime;// 创建时间

    private long modifyTime;// 修改时间

    private String code;// 注册时填写的邀请码

    @Data
    public class Grade{

        private int userId;// 上级userId

        private int grade;// 与自己的等级关系
    }
}
