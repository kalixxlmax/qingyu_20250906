package com.basic.im.room.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Lists;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.entity.Config;
import com.basic.utils.DateUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.*;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;


@ApiModel("房间")
@Data
@Document(value = "chat_room")
public class Room {

	public static String getDBName(){
		return "chat_room";
	}

	//房间编号
	@ApiModelProperty("房间编号")
	@Id
	private ObjectId id;
	@ApiModelProperty("群的id")
	@Indexed
	private String jid; //群的id
	// 房间名称
	@ApiModelProperty("房间名称")
	private String name;
	// 房间描述
	@ApiModelProperty("房间描述")
	private String desc;
	// 房间主题
	@ApiModelProperty("房间主题")
	private String subject;
	// 房间分类
	@ApiModelProperty("房间分类")
	private Integer category;
	// 房间标签
	@ApiModelProperty("房间标签")
	private List<String> tags;
	//语音通话标识符
	@ApiModelProperty("语音通话标识符")
	private String call;
	//视频会议标识符
	@ApiModelProperty("视频会议标识符")
	private String videoMeetingNo;

	// 房间公告
	@ApiModelProperty("房间公告")
	private Notice notice;
	// 公告列表
	@ApiModelProperty("公告列表")
	private List<Notice> notices;

	// 当前成员数
	@ApiModelProperty("当前成员数")
	private Integer userSize;
	// 最大成员数
	@ApiModelProperty("最大成员数")
	private Integer maxUserSize = 1000;
	// 自己
	@ApiModelProperty("自己")
	@Transient
	private Member member;
	// 成员列表
	@ApiModelProperty("成员列表")
	@Transient
	private List<Member> members;

	@ApiModelProperty("国家Id")
	private Integer countryId;// 国家Id
	@ApiModelProperty("省份Id")
	private Integer provinceId;// 省份Id
	@ApiModelProperty("城市Id")
	private Integer cityId;// 城市Id
	@ApiModelProperty("地区Id")
	private Integer areaId;// 地区Id

    @Transient
    @ApiModelProperty("经度")
	private Double longitude;// 经度
    @Transient
	@ApiModelProperty("纬度")
	private Double latitude;// 纬度

    @ApiModelProperty("地理位置")
	@GeoSpatialIndexed(type = GeoSpatialIndexType.GEO_2DSPHERE)
    private Loc loc;

	@ApiModelProperty("面对面建群key")
	private String localRoomKey;// 面对面建群key

	// 群主
	@ApiModelProperty("创建者Id")
	@Indexed
	private Integer userId;

	// 创建者昵称
	@ApiModelProperty("创建者昵称")
	private String nickname;

	// 创建时间
	@ApiModelProperty("创建时间")
	private Long createTime;
	// 修改人
	@ApiModelProperty("修改人")
	private Integer modifier;
	// 修改时间
	@ApiModelProperty("修改时间")
	private Long modifyTime;

	@ApiModelProperty("状态  1:正常, -1:被禁用")
	@Indexed
	private byte s = 1;// 状态  1:正常, -1:被禁用
	@ApiModelProperty("是否可见   0为可见   1为不可见")
	private byte isLook=1;// 是否可见   0为可见   1为不可见
	@ApiModelProperty("群主设置 群内消息是否发送已读 回执 显示数量  0 ：不发生 1：发送")
	private byte showRead=0;   // 群主设置 群内消息是否发送已读 回执 显示数量  0 ：不发生 1：发送
	@ApiModelProperty("加群是否需要通过验证  0：不要   1：要")
	private byte isNeedVerify=0; // 加群是否需要通过验证  0：不要   1：要
	@ApiModelProperty("显示群成员给 普通用户   1 显示  0  不显示")
	private byte showMember=1;// 显示群成员给 普通用户   1 显示  0  不显示
	@ApiModelProperty("允许发送名片 好友  1 允许  0  不允许")
	private byte allowSendCard=1;// 允许发送名片 好友  1 允许  0  不允许
	@ApiModelProperty("是否允许群主修改 群属性")
	private byte allowHostUpdate=1;// 是否允许群主修改 群属性
	@ApiModelProperty("允许普通成员邀请好友  默认 允许")
	private byte allowInviteFriend=1;// 允许普通成员邀请好友  默认 允许
	@ApiModelProperty("允许群成员上传群共享文件")
	private byte allowUploadFile=1;// 允许群成员上传群共享文件
	@ApiModelProperty("允许群成员 开启 讲课")
	private byte allowConference=1;// 允许成员 召开会议
	@ApiModelProperty("")
	private byte allowSpeakCourse=1;// 允许群成员 开启 讲课
	@ApiModelProperty("群组减员发送通知  0:关闭 ，1：开启")
	private byte isAttritionNotice=1;// 群组减员发送通知  0:关闭 ，1：开启
	@ApiModelProperty("大于当前时间时禁止发言")
	private long talkTime; // 大于当前时间时禁止发言
	@ApiModelProperty("-1.0永久保存    1.0保存一天   365.0保存一年")
	private double chatRecordTimeOut=-1; // -1.0永久保存    1.0保存一天   365.0保存一年
	@ApiModelProperty("标签名称")
	private String labelName;
	@ApiModelProperty("推广链接")
	private String promotionUrl;   //推广链接
	@ApiModelProperty("是否为秘密群组，用于群消息的端到端加密   1:秘密群组  0:非秘密群组")
	private byte isSecretGroup = 0;  //是否为秘密群组，用于群消息的端到端加密   1:秘密群组  0:非秘密群组

	//针对该群组的消息加密方式
	@ApiModelProperty("针对该群组的消息加密方式")
	private byte  encryptType = 0; // 0:明文传输 1:des加密传输  2:aes加密传输  3:端到端加密传输
	@ApiModelProperty("发起群组内直播的用户Id")
	private Integer liveUserId;
	@ApiModelProperty("发起群组内直播的用户昵称")
	private String liveUserName;
	@ApiModelProperty("当前群组内直播的状态， 1:开启直播 0:关闭直播")
	private byte liveStatus = 0;
	@ApiModelProperty("允许群成员开启群组直播 ,1:允许  0:不允许")
	private byte allowOpenLive = 1;
	@ApiModelProperty("发起群组内音视频会议的用户昵称")
	private String meetingUserName;
	@ApiModelProperty("当前群组内音视频会议的状态， 0:未开启 1：语音会议 2：视频会议")
	private byte meetingStatus = 0;

    /*@ApiModelProperty("网址Url,管理员建群可设置多个对应的网站")
    private String roomTitleUrl;*/

	@ApiModelProperty("消息撤回的删除时间 单位秒")
	private int withdrawTime;

	@ApiModelProperty("最多可以设置管理员的个数,0:表示没有限制")
	private int adminMaxNumber;

	@ApiModelProperty("是否已经加入该群")
	@Transient
	private boolean inGroup;
	@ApiModelProperty("是否为付费群组，付费才能加入群聊   1:付费群组  0:非付费群组")
	private byte needPay = 0;  //是否为付费群组，付费才能加入群聊   1:付费群组  0:非付费群组
	@ApiModelProperty("付费群组,购买能进入群聊的天数")
	private int payForDays = 0;  //付费群组,购买能进入群聊的天数
	@ApiModelProperty("付费金额")
	private int payForAmount;
	/**
	 * 标志位
	 */
	//private boolean falg;

	@Data
	@Accessors(chain = true)
	public static class Loc {
		private double lng;// longitude  经度
		private double lat;// latitude   纬度
	}

	/**
	 * 初始化群组配置
	 */
	public void initRoomConfig(int createrId, String createrNickName, Config config) {

		if(null==this.getId()) {
            this.setId(ObjectId.get());
        }

		//this.setCall(String.valueOf(SKBeanUtils.getUserManager().createCall()));
		//this.setVideoMeetingNo(String.valueOf(SKBeanUtils.getUserManager().createvideoMeetingNo()));

		this.setSubject("");
		this.setTags(Lists.newArrayList());
		if (null == this.notice) {
			this.setNotice(new Notice());
		}
		this.setNotices(Lists.newArrayList());
		this.setUserSize(0);
		this.setMembers(Lists.newArrayList());

		this.setUserId(createrId);
		this.setNickname(createrNickName);
		this.setCreateTime(DateUtil.currentTimeSeconds());
		this.setModifyTime(this.getCreateTime());
		this.setS((byte)1);
		this.setTalkTime(0);// 初始化全体禁言
		if(config.getMaxUserSize() > 0) {
            this.setMaxUserSize(config.getMaxUserSize());
        }

		this.setIsAttritionNotice(config.getIsAttritionNotice());
		this.setIsLook(config.getIsLook());
		this.setShowRead(config.getShowRead());
		this.setIsNeedVerify(config.getIsNeedVerify());
		this.setShowMember(config.getShowMember());
		this.setAllowSendCard(config.getAllowSendCard());
		this.setAllowInviteFriend(config.getAllowInviteFriend());
		this.setAllowUploadFile(config.getAllowUploadFile());
		this.setAllowConference(config.getAllowConference());
		this.setAllowSpeakCourse(config.getAllowSpeakCourse());
		this.setAllowOpenLive((byte)1);
		this.setAdminMaxNumber(config.getAdminMaxNumber());
	}

	public synchronized void addMember(Member member) {
		if(null==members||0==members.size()) {
			members=new ArrayList<Member>();
			members.add(member);
		}else {

			boolean contains=false;
			for (Member mem : members) {
				if(mem.userId.equals(member.getUserId())) {
					contains=true;
					break;
				}
			}
			if(!contains) {
                members.add(member);
            }

		}


	}

	public synchronized void removeMember(Member member) {
		if(null!=members||0<members.size()) {
			members.remove(member);
		}
	}

	public synchronized  void removeMember(int userId) {
		if(null!=members||0<members.size()) {
			Member member=null;
			for (Member mem : members) {
				if(mem.userId.equals(userId)) {
					member=mem;
					break;
				}
			}
			if(null!=member) {
                members.remove(member);
            }
		}
	}




	@Data
	@Document(value = "chat_room_notice")
	public static class Notice {
		@Id
		private ObjectId id;// 通知Id

//		@JSONField(serialize = false)
		private ObjectId roomId;// 房间Id
		private String text;// 通知文本
		private Integer userId;// 用户Id
		private String nickname;// 用户昵称
		private long time;// 时间
		private long modifyTime;// 修改时间

		public Notice() {

		}

		public Notice(ObjectId id,ObjectId roomId, String text, Integer userId, String nickname) {
			this.id = id;
			this.roomId = roomId;
			this.text = text;
			this.userId = userId;
			this.nickname = nickname;
			this.time = DateUtil.currentTimeSeconds();
		}

	}

	@ApiModel("成员")
	@Data
	@Document(value = "chat_room_member")
	@CompoundIndexes({
			@CompoundIndex(def = "{'roomId':1,'userId':1}"),
			@CompoundIndex(def = "{'roomId':1,'userId':1,'role':1}"),
			@CompoundIndex(def = "{'roomId':1,'userId':1,'nickname':1}")
	})
	public static class Member {
		@Id
		@JSONField(serialize = false)
		@ApiModelProperty("编号")
		private ObjectId id;

		// 房间Id
		@JSONField(serialize = false)
		@ApiModelProperty("房间Id")
		@Indexed
		private ObjectId roomId;

		// 成员Id
		@ApiModelProperty("成员Id")
		@Indexed
		private Integer userId;

		// 成员昵称
		@ApiModelProperty("成员昵称")
		@Indexed
		private String nickname;

		//群主、管理员 备注 成员名称
		@ApiModelProperty("群主、管理员 备注 成员名称")
		private String remarkName;

		// 成员角色：1=创建者、2=管理员、3=普通成员、4=隐身人、5=监控人
		@ApiModelProperty("成员角色：1=创建者、2=管理员、3=普通成员、4=隐身人、5=监控人")
		private int role=3;

		// 订阅群信息：0=否、1=是
		@ApiModelProperty("订阅群信息：0=否、1=是")
		private Integer sub;

		//语音通话标识符
		@ApiModelProperty("语音通话标识符")
		private String call;

		//视频会议标识符
		@ApiModelProperty("视频会议标识符")
		private String videoMeetingNo;

		//消息免打扰（1=是；0=否）
		@ApiModelProperty("消息免打扰（1=是；0=否）")
		private Integer offlineNoPushMsg=0;

		// 大于当前时间时禁止发言
		@ApiModelProperty("大于当前时间时禁止发言")
		private Long talkTime;

		// 最后一次互动时间
		@ApiModelProperty("最后一次互动时间")
		private Long active;

		/**
		 *加入群组时序列号
		 */
		@ApiModelProperty("加入群组时序列号")
		private long joinSeqNo = 0;
		// 创建时间
		@ApiModelProperty("创建时间")
		private Long createTime;

		// 修改时间
		@ApiModelProperty("修改时间")
		private Long modifyTime;

		// 是否开启置顶聊天 0：关闭，1：开启
		@ApiModelProperty("是否开启置顶聊天 0：关闭，1：开启")
		private byte isOpenTopChat = 0;

		// 开启置顶聊天时间
		@ApiModelProperty("开启置顶聊天时间")
		private long openTopChatTime = 0;

		/**
		 * beginMsgTime > createTime
		 * 优先使用 beginMsgTime
		 */
		@ApiModelProperty("清空群组聊天消息的时间")
		private long beginMsgTime = 0;
		/**
		 * 付费群使用的,到期之后,无法进入群聊
		 */
		@ApiModelProperty("期限")
		private Long deadLine = -1L;

		/**
		 * 清空群组聊天最大序列号
		 *
		 */
		@ApiModelProperty("清空群组聊天最大序列号")
		private long clearMaxSeqNo = 0;


		//群消息端到端加密，群成员的chatKey
		@ApiModelProperty("群消息端到端加密，群成员的chatKey")
		private String chatKeyGroup;

		@Transient
		@ApiModelProperty("最后登录时间")
		private long loginTime;// 最后登录时间

		@Transient
		@ApiModelProperty("ip地址")
		private String ipAddress;// ip地址

		@Transient
		@ApiModelProperty("在线状态")
		private Integer onlinestate = 0;   //在线状态，默认离线0  在线 1


		@ApiModelProperty("加群方式")
		private Integer operationType=0;    // 见下方 OperationType 定义

		@ApiModelProperty("邀请者Id")
		private Integer inviteUserId=0;    // 如果是邀请进群，则记录邀请者的Id

		@Transient
		private String inviteUserNikeName;  // 邀请者名称

		@Transient
		private String AddRoomDetails;   // 群成员进群信息

		@ApiModelProperty("是否被拉黑")
		private byte isBlack=0;            // 是否被拉黑

		@Transient
		private int hiding=0; // 隐身模式，0关闭，1开启


		/**
		 * 隐藏群组会话 0：关闭 1开启 默认值 0
		 */
		private byte hideChatSwitch=0;


		@Transient
		private String  account;



		/**
		 * 是否已读群公告
		 * 发布新公告，该值设置为0
		 * 用户已读公告，该值设置为1
		 */
		private byte readNotice=0;


		public interface OperationType{
			int SEARCH = 1;          // 搜索加群
			int SEARCH_PAY = 2;      // 搜索付费加群
			int SWEEP_CODE = 3;      // 扫码入群
			int SWEEP_CODE_PAY = 4;  // 扫码付费入群
			int INVITE = 5;          // 邀请入群
			int INVITE_PAY=6;        // 邀请付费入群
			int PAY_INVITE = 7;      // 付费邀请入群
			int SYSTEM_INVITE=8;     // 系统邀请
			int NEAR_ROOM=9;         // 附近的群
			int NEAR_ROOM_PAY=10;    // 附近付费加群
			int RANK_ROOM=11;        // 榜单加群
			int RANK_ROOM_PAY=12;    // 榜单付费加群



			int OTHER=-1;            // 通过其他方式加群
		}


		public Member() {}

		public Member(ObjectId roomId, Integer userId, String nickname) {
			this.active= DateUtil.currentTimeSeconds();
			this.roomId = roomId;
			this.userId = userId;
			this.nickname = nickname;
			this.role = KConstants.Room_Role.MEMBER;
			this.sub = 1;
			this.talkTime = 0L;
			this.createTime = this.active;
			this.modifyTime = this.active;
		}

		public Member(ObjectId roomId, Integer userId, String nickname,long currentTime){
			this.active= DateUtil.currentTimeSeconds();
			this.roomId = roomId;
			this.userId = userId;
			this.nickname = nickname;
			this.role = KConstants.Room_Role.MEMBER;
			this.sub = 1;
			this.talkTime = 0L;
			this.createTime = this.active;
			this.modifyTime = this.active;
			this.createTime = currentTime;
		}

	}

	@Data
	@Document(value="chat_room_share")
	public static class Share {

		@Id
		private ObjectId shareId;//id
		@JSONField(serialize = false)
		private  ObjectId roomId;
		private String name;//文件名称
		private String url;//文件路径
		private long time;//发送时间
		@Indexed
		private  Integer userId;//发消息的用户id
		private String nickname;//昵称
		private int type;//文件类型()
		private float size;//文件大小

		public Share() {}

		public Share(ObjectId shareId, ObjectId roomId, String name, String url, long time, Integer userId,
				String nickname, int type, float size) {
			this.shareId = shareId;
			this.roomId = roomId;
			this.name = name;
			this.url = url;
			this.time = time;
			this.userId = userId;
			this.nickname = nickname;
			this.type = type;
			this.size = size;
		}

	}


	/* 新加入字段 */

	/**
	 * 	允许修改群名片
	 * 	1允许修改群名片; 0禁止修改群名片
	 **/
	private byte allowModifyCard = 1;

	/**
	 * 是否显示水印开关
	 * 1=显示水印, 0=关闭水印
	 **/
	private byte showMarker;
}
