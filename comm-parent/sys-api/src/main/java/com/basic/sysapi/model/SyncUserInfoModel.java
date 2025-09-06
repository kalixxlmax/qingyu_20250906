package com.basic.sysapi.model;

/**
 * 同步注册用户 模型 <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/11  <br>
 */

public class SyncUserInfoModel {

    /**
     * 第三方平台用户ID唯一:注册及修改必需字段
     */

    private String thirdId;

    /**
     * 扩展字段，json 字符串 base64
     */
    private String extension;

    /**
     * 手机区号:注册必需字段
     */
    private String areaCode;

    /**
     * 手机号码,不加区号:注册必需字段
     */
    private String telephone;

    /**
     * 登陆密码,明文MD5加密:注册必需字段
     */
    private String password;

    /**
     * 用户昵称:注册必需字段
     */
    private String nickname;

    /**
     * 性别  0:女 1:男
     */
    private Integer sex;


    /**
     * 生日,日期秒单位
     */
    private Long birthday;


    /**
     * 账号,通讯号:为空默认随机生成
     */
    private String account;

    /**
     * 个人签名信息:默认为空
     */
    private String description;

    /**
     * 支付密码：不能用明文:为空则不设置
     */
    private String payPassWord;




    public String getThirdId() {
        return thirdId;
    }

    public void setThirdId(String thirdId) {
        this.thirdId = thirdId;
    }

    public String getExtension() {
        return extension;
    }

    public void setExtension(String extension) {
        this.extension = extension;
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

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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
}
