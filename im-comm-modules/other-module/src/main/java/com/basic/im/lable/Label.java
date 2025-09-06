package com.basic.im.lable;


import org.bson.types.ObjectId;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 *     群标识码
 */

@ApiModel("群标识码")
@Document(value = "label")
public class Label {
    //群标识码ID
	@ApiModelProperty("群标识码ID")
    @Id
    private  ObjectId id;
    //创建人
	@ApiModelProperty("创建人")
    @Indexed
    private Integer  userId;

    //标识码（系统自动生成）
	@ApiModelProperty("标识码")
    @Indexed(unique=true)
    private String  code;
    //群标识名称
	@ApiModelProperty("群标识名称")
    @Indexed(unique=true)
    private String  name;
    //存放图片文件地址
	@ApiModelProperty("存放图片文件地址")
    private String  logo;
    //备注
	@ApiModelProperty("备注")
    private String  mark;


    public Label(){ }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
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

    public String getMark() {
        return mark;
    }

    public void setMark(String mark) {
        this.mark = mark;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }


}
