package com.basic.im.admin.entity;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(value="keyWord")
public class KeyWord {
	@Id
	private ObjectId id;
	private String word;
	private short type; //type = 0 关键词  type =1 否词
	private long createTime;

	public ObjectId getId() {
		return id;
	}
	public void setId(ObjectId id) {
		this.id = id;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}


	public long getCreateTime() {
		return createTime;
	}
	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}


	public short getType() {
		return type;
	}

	public void setType(short type) {
		this.type = type;
	}
}
