package com.basic.sysapi.model;


import java.util.Map;

/**
 * 同步注册用户 模型 <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/11  <br>
 */
public class QueryUserInfoResult {

    /**
     *第三方平台用户ID
     */
    private String thirdId;

    /**
     *聊天系统UserId
     */
    private Integer userId;
    /**
     *手机区号
     */
    private String areaCode;

    /**
     *手机号码
     */
    private String telephone;

    /**
     *用户昵称
     */
    private String nickname;

    /**
     *性别  0:女 1:男
     */
    private Integer sex;

    /**
     *生日,日期秒单位
     */
    private Long birthday;

    /**
     *账号,通讯号
     */
    private String account;
    /**
     *个人签名信息
     */
    private String description;
    /**
     *支付密码: 0: 未设置 1:已设置
     */
    private String payPassWord;

    /**
     * 第三方 额外扩展字段
     */
    public Map<String,String> thirdExtension;


    public String getThirdId() {
        return thirdId;
    }

    public void setThirdId(String thirdId) {
        this.thirdId = thirdId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getAreaCode() {
        return areaCode;
    }

    public void setAreaCode(String areaCode) {
        this.areaCode = areaCode;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public Long getBirthday() {
        return birthday;
    }

    public void setBirthday(Long birthday) {
        this.birthday = birthday;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getPayPassWord() {
        return payPassWord;
    }

    public void setPayPassWord(String payPassWord) {
        this.payPassWord = payPassWord;
    }

    public Map<String, String> getThirdExtension() {
        return thirdExtension;
    }

    public void setThirdExtension(Map<String, String> thirdExtension) {
        this.thirdExtension = thirdExtension;
    }
}
