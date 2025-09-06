package com.basic.im.push.vo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Getter;
import lombok.Setter;

/**
 * @description: 通讯协议消息内容 <br>
 * @date: 2020/7/27 0027  <br>
 * @author: lidaye <br>
 * @version: 1.0 <br>
 */
@Getter
@Setter
public class PushMessageDTO {


    private String content;

    private String fileName;

    private long fromUserId;

    private String fromUserName;

    private String objectId;

    private Number timeSend;

    private String toUserId;

    private String toUserName;

    private int fileSize;

    private int type;

    private String messageId;

    //客服id,用于客服模块相关消息记录客服id
    private Long srvId;

    /**
     * 外面的to 消息发送给谁
     */
    private String to;

    private String roomJid;

    @JSONField(name = "isGroup")
    private boolean isGroup;


    private byte isReadDel;

    private byte isEncrypt;


    private byte encryptType;


    /**
     * 推送包名
     */
    private String pushPackageName;

    /**
     * 推送类别
     */
    private int pushType;

    /**
     * 推送 title
     */
    private String title;


    private String pushToken;



}
