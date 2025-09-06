package com.basic.im.admin.entity;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * 关键词拦截记录
 */
@Document(value="keywordDenyRecord")
public class KeywordDenyRecord {
	@Id
	private ObjectId id;

	@Indexed
	private int fromUserId; //发送者id

	private String fromUserName; //发送者昵称

	@Indexed
	private int toUserId; //接收者id

	private String toUserName; //接收者昵称

	@Indexed
	private String roomJid;  //群组jid

	@Indexed
	private String messageId; //对应的消息id

	private String msgContent; //消息内容

	private  String keyword;//包含的敏感词

	private short keywordType; //敏感词类型  0 普通敏感词  1 否词

	private short chatType = 1; //chatType = 1 单聊   chatType = 2 群聊

	private long createTime;


	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public int getFromUserId() {
		return fromUserId;
	}

	public void setFromUserId(int fromUserId) {
		this.fromUserId = fromUserId;
	}

	public int getToUserId() {
		return toUserId;
	}

	public void setToUserId(int toUserId) {
		this.toUserId = toUserId;
	}

	public String getRoomJid() {
		return roomJid;
	}

	public void setRoomJid(String roomJid) {
		this.roomJid = roomJid;
	}

	public String getMsgContent() {
		return msgContent;
	}

	public void setMsgContent(String msgContent) {
		this.msgContent = msgContent;
	}

	public short getChatType() {
		return chatType;
	}

	public void setChatType(short chatType) {
		this.chatType = chatType;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public String getMessageId() {
		return messageId;
	}

	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}

	public String getFromUserName() {
		return fromUserName;
	}

	public void setFromUserName(String fromUserName) {
		this.fromUserName = fromUserName;
	}

	public String getToUserName() {
		return toUserName;
	}

	public void setToUserName(String toUserName) {
		this.toUserName = toUserName;
	}

	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public short getKeywordType() {
		return keywordType;
	}

	public void setKeywordType(short keywordType) {
		this.keywordType = keywordType;
	}
}
