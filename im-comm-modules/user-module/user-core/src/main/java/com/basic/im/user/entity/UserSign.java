package com.basic.im.user.entity;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * @titel:
 * @author:
 * @package: cn.tig.im.vo.UserSign
 * @fileName: UserSign.java
 * @dateTime: 2019/7/5 14:44
 * @description:
 **/

@Document(value = "UserSign")
public class UserSign {

    @Id
    private ObjectId id;
    @Indexed
    private String userId;
    private Date signDate;
    private String device;
    private String signIP;
    private String signAward;
    private Integer status;
    private Date createDate;
    private Date updateDate;
    private String userName;
    private String userAccount;
    private String userPhone;
    private String ipAddress;

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

    public Date getSignDate() {
        if(this.signDate != null) {
            return new Date(this.signDate.getTime());
        } else {
            return null;
        }
    }

    public void setSignDate(Date signDate) {
        if(signDate != null){
            this.signDate = new Date(signDate.getTime());
        }
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getSignIP() {
        return signIP;
    }

    public void setSignIP(String signIP) {
        this.signIP = signIP;
    }

    public String getSignAward() {
        return signAward;
    }

    public void setSignAward(String signAward) {
        this.signAward = signAward;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Date getCreateDate() {
        if(this.createDate != null) {
            return new Date(this.createDate.getTime());
        } else {
            return null;
        }
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

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserAccount() {
        return userAccount;
    }

    public void setUserAccount(String userAccount) {
        this.userAccount = userAccount;
    }

    public String getUserPhone() {
        return userPhone;
    }

    public void setUserPhone(String userPhone) {
        this.userPhone = userPhone;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
}
