package com.basic.im.msg.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;


@Data
@Document(value="FxSetting")
public class FxSetting {
	@Id
	private ObjectId id;
	public String icon;
    public String name;
    public String url;
    public Integer sort;
}
