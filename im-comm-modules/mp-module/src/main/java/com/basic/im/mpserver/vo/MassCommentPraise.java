package com.basic.im.mpserver.vo;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @ClassName MassCommentPraise
 * @Author xie yuan yang
 * @date 2020.12.23 09:30
 * @Description 评论、文章点赞
 */
@Data
@Document(value = "massCommentPraise")
public class MassCommentPraise {

    private @Id
    ObjectId id;

    /**
     * 点赞用户ID
     **/
    private long userId;

    /**
     * 评论、文章编号
     **/
    private String contentId;

    /**
     * 时间
     */
    private long createTime;


    public MassCommentPraise(long userId, String contentId) {
        this.userId = userId;
        this.contentId = contentId;
    }
}
