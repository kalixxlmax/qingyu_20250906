package com.basic.im.mpserver.vo;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @ClassName MassContent
 * @Author xie yuan yang
 * @date 2020.12.14 12:27
 * @Description 富文本群发实体
 */
@Data
@Document(value = "massContent")
public class MassContent{
    private @Id
    ObjectId id;

    /**
     * 内容
     **/
    private String content;

    /**
     * 标题
     **/
    private String title;

    /**
     * 小标题
     **/
    private String sub;

    /**
     * 来源
     **/
    private String contentFrom;

    /**
     * 点赞数量
     */
    private int praise;

    /**
     * 阅读数量
     */
    private int look;

    /**
     * 创建时间
     **/
    private long createTime;

    public MassContent(ObjectId id, String content, String title, String sub, String contentFrom) {
        this.id = id;
        this.content = content;
        this.title = title;
        this.sub = sub;
        this.contentFrom = contentFrom;
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
