package com.basic.im.open.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

/**
 * @author xie yuan yang
 * @Date Created in 2019/9/16 9:51
 * @description 公众号账户信息
 * @modified By:
 */
@Data
public class OfficialInfo {
    @Id
    private ObjectId id;
    @Indexed
    private String telephone;//手机号码
    private String areaCode="86";
    private String password;//密码
    private String companyName;//公司名称
    private String companyBusinessLicense;//公司营业执照
    private String adminName;//管理员名称
    private String adminID;//管理员身份证号码
    private long createTime;//创建用户时间
    protected String country;// 国家
    protected String province;// 省份
    protected String city;// 城市名称
    protected String desc;// 详细地址
    private int verify=0;//审核 0--未审核  1--审核通过  2--审核不通过
    private String feedback;//反馈
    private String industryImg;//工商执照图片
    private String identityName;//用户姓名
    private String identity;//用户身份证号码
    private String positiveUrl;//身份证正面图片
    private String negativeUrl;//身份证反面图片
    private Integer userId;
    private String phone;
    // 用户类型：0=普通用户；  2=公众号-个人号  3-企业号；
    private Integer officialType=0;

    private String officialHeadImg;//公众号头像


    private int companyType;//公司类型 0--个体工商户  1--企业
    private String adminTelephone;//管理员手机号码
}
