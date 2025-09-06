package com.basic.im.user.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * 客户端授权的IP记录 <br>
 *
 * @author: lidaye <br>
 * @date: 2021/3/27 0027  <br>
 */
@Data
@Document(value ="allow_request_client" )
public class AllowRequestClient {

    @Id
    private ObjectId id;


    @Indexed
    private String ip;


    /**
     * 描述
     */
    private String desc;


    /*
    @Indexed
    private byte type;*/


    /**
     * 状态  1:状态 -1:禁用
     */
    @Indexed
    private Byte status;

    private long modifTime;

    private long createTime;
}
