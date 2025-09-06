package com.basic.im.live.entity;

import com.alibaba.fastjson.annotation.JSONField;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Set;

//直播间
@ApiModel("直播间")
@Data
@Document(value="LiveRoom")
public class LiveRoom {
	@ApiModelProperty("房间编号")
	private @Id ObjectId roomId;
	
	@Indexed
	@ApiModelProperty("直播间创建者")
	private Integer userId; //直播间创建者
	@ApiModelProperty("昵称")
	private String nickName;
	@ApiModelProperty("直播间名称")
	private String name;//直播间名称
	@ApiModelProperty("房间公告")
	private String notice;//房间公告
	@ApiModelProperty("直播间推流地址")
	private String url;//直播间推流地址
	@ApiModelProperty("房间封面")
	private String img;//房间封面
	@ApiModelProperty("直播间人数")
	private Integer numbers=0;//直播间人数
	@ApiModelProperty("直播状态 1:开启直播 0:关闭直播")
	private Integer status=0;//直播状态 1:开启直播 0:关闭直播
	@ApiModelProperty("直播间当前状态 0：正常, 1：禁用 ")
	private Integer currentState = 0;// 直播间当前状态 0：正常, 1：禁用 
	@ApiModelProperty("创建时间")
	private long createTime=0;//创建时间
	@ApiModelProperty("编号")
	private String jid;
	@ApiModelProperty("当前直播的群组Id")
	private String liveRoomId;


	private long startTime=0;//开始时间

	private long endTime=0;//结束时间

	/**
	 * 当前商品ID
	 */
	private String productId;


	private List<String> productIdList;

	/**
	 * 观众数量
	 */
	private int audienceCount;

	/**
	 * 礼物总金额
	 */
	@JSONField(format ="0.00")
	private double rewardCount;

	/**
	 * 分享数量
	 */
	private int shareCount;


	/**
	 * 商品点击次数
	 */
	private int productClickCount;

	/**
	 * 下单数量
	 */
	private int orderCount;


	/**
	 * 下单总金额
	 */
	@JSONField(format ="0.00")
	private double orderMoneyCount;



	public LiveRoom() {
		
	}
	
	public LiveRoom(ObjectId roomId, String name, String notice, String url, Integer currentState) {
		this.roomId = roomId;
		this.name = name;
		this.notice = notice;
		this.url = url;
		this.currentState = currentState;
	}





	//直播间的用户
	@Document(value="LiveRoomMember")
	@Data
	public static class LiveRoomMember {
		
		@Id
		private ObjectId id;
		@Indexed
		private ObjectId roomId;
		@Indexed
		private Integer userId;

		private String nickName;

		private int type=3;//1为创建者 2为管理员 3为成员

		private int state=0;	//为了判断是否被禁言1为禁言，0为未禁言

		private long talkTime;// 大于当前时间时禁止发言

		private double rewardCount;//打赏金额
		
		private long createTime;//加入时间

		private int online = 1;//是否在线  1为在线0为退出
	}
}
