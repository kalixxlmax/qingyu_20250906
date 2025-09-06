package com.basic.im.mpserver.vo;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @ClassName MassComment
 * @Author xie yuan yang
 * @date 2020.12.14 12:30
 * @Description
 */
@Data
@Document(value = "massComment")
public class MassComment {

    private @Id
    ObjectId id;

    /**
     * 内容编号
     **/
    private String massContentId;

    /**
     * 用户ID
     **/
    private long userId;

    /**
     * 用户昵称
     **/
    private String userName;

    /**
     * 评论内容
     **/
    private String commentContent;

    /**
     * 点赞人数
     **/
    private int praise;

    /**
     * 创建时间
     **/
    private long createTime;


    public MassComment(String massContentId, long userId, String userName, String commentContent) {
        this.massContentId = massContentId;
        this.userId = userId;
        this.userName = userName;
        this.commentContent = commentContent;
    }


    /**
     * 是否已经点赞
     */
    @Transient
    private int isPraise;


    public interface Type{
        int PRAISE = 1;         //点赞
        int NOT_PRAISE = 0;     //未点赞
    }
}
