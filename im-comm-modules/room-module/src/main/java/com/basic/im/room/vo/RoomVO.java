package com.basic.im.room.vo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.bson.types.ObjectId;

/**
* @Description: TODO(群组  数据类)
* @author lidaye
* @date 2018年6月20日 
*/
@ApiModel("群组  数据类")
@Data
public class RoomVO {
	@ApiModelProperty("群编号")
	private ObjectId roomId;
	@ApiModelProperty("群名称")
	private String roomName="";
	@ApiModelProperty("注意")
	private String notice="";
	@ApiModelProperty("详情")
	private String desc="";
	@ApiModelProperty("主题")
	private String subject="";
	@ApiModelProperty("用户编号")
	private Integer userId;
	@ApiModelProperty("显示已读")
	private int showRead=-1;
	@ApiModelProperty("加群是否需要通过验证  0：不要   1：要")
	private int isNeedVerify=-1;// 加群是否需要通过验证  0：不要   1：要

	@ApiModelProperty("是否可见   0为可见   1为不可见")
	private Integer isLook=-1;//是否可见   0为可见   1为不可见
	@ApiModelProperty("最大成员数")
	private Integer maxUserSize;	// 最大成员数
	@ApiModelProperty("显示群成功给 普通用户   1 显示  0  不显示")
	private int showMember=-1;//显示群成功给 普通用户   1 显示  0  不显示
	@ApiModelProperty("允许发送名片 好友  1 允许  0  不允许")
	private int allowSendCard=-1;//允许发送名片 好友  1 允许  0  不允许
	@ApiModelProperty("是否允许群主修改 群属性")
	private int allowHostUpdate=-1;//是否允许群主修改 群属性
	@ApiModelProperty("聊天记录时间")
	private double chatRecordTimeOut=0;
	@ApiModelProperty("允许普通成员邀请好友  默认 允许")
	private int allowInviteFriend=-1;//允许普通成员邀请好友  默认 允许
	@ApiModelProperty("允许群成员上传群共享文件")
	private int allowUploadFile=-1;//允许群成员上传群共享文件
	@ApiModelProperty("允许成员 召开会议")
	private int allowConference=-1;//允许成员 召开会议
	@ApiModelProperty("允许群成员 开启 讲课")
	private int allowSpeakCourse=-1;//允许群成员 开启 讲课
	@ApiModelProperty("群组减员发送通知  0:关闭 ，1：开启")
	private int isAttritionNotice=-1;// 群组减员发送通知  0:关闭 ，1：开启
	
	// 大于当前时间时禁止发言
	@ApiModelProperty("大于当前时间时禁止发言")
	private long talkTime=-2;

	@ApiModelProperty("群组状态  1：正常，-1：被禁用")
	private Integer s;// 群组状态  1：正常，-1：被禁用

	@ApiModelProperty("允许群成员开启群组直播 ,1:允许  0:不允许")
	private byte allowOpenLive = -1;

	@ApiModelProperty("网址Url,管理员建群可设置多个对应的网站")
	private String roomTitleUrl;

	@ApiModelProperty("消息撤回的删除时间 单位秒")
	private int withdrawTime = -1;

	@ApiModelProperty("最多可以设置管理员的个数,0:表示没有限制")
	private int adminMaxNumber=-1;

	@ApiModelProperty("是否禁用群成员修改名片功能 0=允许修改群名片, 1=禁止修改群名片")
	private byte allowModifyCard = -1;

	@ApiModelProperty("是否显示水印开关 1=显示水印, 0=关闭水印")
	private byte showMarker = -1;
}

