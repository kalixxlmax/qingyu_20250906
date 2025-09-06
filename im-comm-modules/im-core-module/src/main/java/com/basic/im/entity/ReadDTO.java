package com.basic.im.entity;

import lombok.Data;

import java.util.List;

@Data
public class ReadDTO {

    /**
     * 群组 JID
     */
    private String roomJid;

    /**
     * 用户消息列表
     * @see ReadDTO.Message
     */
    private List<Message> messageList;


    /**
     * 用户消息实体
     */
    @Data
    public static class Message{
        /**
         * 消息 ID
         */
        private String messageId;
        /**
         * 已读总数
         */
        private Integer count;
    }
}