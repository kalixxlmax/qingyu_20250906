package com.basic.im.user.entity;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * @titel:
 * @author:
 * @package: cn.tig.im.vo.UserSignInfo
 * @fileName: UserSignInfo.java
 * @dateTime: 2019/7/5 14:44
 * @description:
 **/
@Document(value = "UserSignInfo")
public class UserSignInfo {

    @Id
    private ObjectId id;
    @Indexed
    private String userId;
    private Integer seriesSignCount;
    private Integer signCount;
    private Integer dialCount;
    private Integer sevenCount;
    private Date startSignDate;
    private Date createDate;
    private Date updateDate;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getSeriesSignCount() {
        return seriesSignCount;
    }

    public void setSeriesSignCount(Integer seriesSignCount) {
        this.seriesSignCount = seriesSignCount;
    }

    public Integer getSignCount() {
        return signCount;
    }

    public void setSignCount(Integer signCount) {
        this.signCount = signCount;
    }

    public Integer getDialCount() {
        return dialCount;
    }

    public void setDialCount(Integer dialCount) {
        this.dialCount = dialCount;
    }

    public Integer getSevenCount() {
        return sevenCount;
    }

    public void setSevenCount(Integer sevenCount) {
        this.sevenCount = sevenCount;
    }

    public Date getStartSignDate() {
        if(this.startSignDate != null)
            return new Date(this.startSignDate.getTime());
        else
            return null;
    }

    public void setStartSignDate(Date startSignDate) {
        if(startSignDate != null){
            this.startSignDate = new Date(startSignDate.getTime());
        }
    }

    public Date getCreateDate() {
        if(this.createDate != null)
            return new Date(this.createDate.getTime());
        else
            return null;
    }

    public void setCreateDate(Date createDate) {
        if(createDate != null){
            this.createDate = new Date(createDate.getTime());
        }
    }

    public Date getUpdateDate() {
        if(this.updateDate != null) {
            return new Date(this.updateDate.getTime());
        } else {
            return null;
        }
    }

    public void setUpdateDate(Date updateDate) {
        if(updateDate != null){
            this.updateDate = new Date(updateDate.getTime());
        }
    }
}
