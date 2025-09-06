/**
 * 
 */
package com.basic.im.user.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author lidaye
 * 2017年6月26日
 */
@Data
@Document(value = "Report")
public class Report {
	@Id
	private  ObjectId id;//

	@Indexed
	private  long userId;// 举报用户
	
	private @Indexed long toUserId=0;// 被举报用户
	
	private String roomId;//群组id 被举报群组

	// 被举报的网页的域名
	private String webUrl;
	
	private int reason;// 原因Id
	
	private long time;// 举报时间
	
	private @Indexed int status=1;

	private int reportType;// 举报类型(0.用来兼容旧版本处理) 1.用户 2.群组 3.网页 4.朋友圈 5.短视频

	private String reportInfo;// 举报的详情，朋友圈的id或者短视频的id

	@Transient
	private int toUserStatus;// 被举报人当前账号状态   -1：锁定, 1:正常
	
	@Transient
	private int roomStatus;// 被举报群组当前状态   -1：锁定, 1:正常
	
//	@Transient
	private int webStatus;// 被举报网页状态  -1：锁定   1：正常
	
	@Transient
	private String info;// 举报原因
	
	@Transient
	private String userName;// 举报人昵称
	
	@Transient
	private String toUserName;// 被举报人昵称
	
	@Transient
	private String roomName;// 被举报的群组昵称

	@Transient
	private String reportMsgContent;// 被举报的内容详情（eg:朋友圈的内容 ）

	//举报网址
	private String webPageUrl;


}
