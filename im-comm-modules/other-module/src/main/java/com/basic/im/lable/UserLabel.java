package com.basic.im.lable;


import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 *     用户群标识
 */
@Document(value = "userlabel")
public class UserLabel {
    @Id
    private ObjectId id;// 关系Id
    @Indexed
    private Integer  userId;//用户Id
    private String   labelId;//标识ID
    private String   name ;
    private String   logo;
    private String   code;
    private long     date;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getLabelId() {
        return labelId;
    }

    public void setLabelId(String labelId) {
        this.labelId = labelId;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLogo() {
        return logo;
    }

    public void setLogo(String logo) {
        this.logo = logo;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
	
	public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }
}
