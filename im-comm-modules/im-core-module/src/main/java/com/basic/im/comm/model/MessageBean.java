package com.basic.im.comm.model;

import com.alibaba.fastjson.JSON;
import lombok.Data;

@Data
public class MessageBean {

    private Object content;
    private String fileName;
    private String fromUserId = "10005";
    private String fromUserName = "10005";
    private Object objectId;
    private Number timeSend;
    private String toUserId;
    private String toUserName;
    private int fileSize;
    private int type;

    private String messageId;

    private String other;

    private int msgType; // 消息type  0：普通单聊消息    1：群组消息    2：广播消息  3:压测消息

    //子type  1 客服消息  2 ios 分享消息,服务器代发
    private byte subType;

    private Long srvId; //客服id,用于客服模块相关消息记录客服id

    private String roomJid;// 群组jid

    private int imSys = 1; // 1 表示系统消息，其它则不是


    /**
     * 外面的to 消息发送给谁
     */
    private String to;

    /**
     * 1=群主聊天刷新, 0=单聊刷新
     **/
    private int timeLen = 1;


    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }




}
