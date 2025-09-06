package com.basic.im.entity;

import lombok.Data;
import lombok.experimental.Accessors;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * 用户指定会话
 */
@Data
@Accessors(chain = true)
@Document("stick_dialog")
public class StickDialog {

    @Id
    private ObjectId id;

    /**
     * 用户ID
     */
    private Integer userId;

    /**
     * JID
     */
    private String  jid;

    /**
     * 是否是群组 0=不是 1=是
     */
    private Integer isRoom;

    /**
     * 置顶时间
     */
    private Long time;
}
