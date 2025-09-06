package com.basic.mianshi.rocketmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.admin.dao.KeywordDAO;
import com.basic.im.admin.entity.KeywordDenyRecord;
import com.basic.im.admin.service.impl.AdminManagerImpl;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.config.AppConfig;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.utils.DateUtil;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.entity.Config;
import com.basic.im.room.entity.Room;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RocketMQMessageListener(topic = "keywordMessage", consumerGroup = "my-consumer-keywordMessage")
public class KeywordConsumer  implements RocketMQListener<String>{

	    private static final Logger log = LoggerFactory.getLogger(KeywordConsumer.class);


		@Autowired
		private AppConfig appConfig;


		@Lazy
		@Autowired
		private KeywordDAO keywordDAO;
		@Lazy
		@Autowired
		protected UserCoreService userCoreService;

		@Lazy
		@Autowired
		private AdminManagerImpl adminManager;

		@Autowired
		@Lazy
		private RoomManagerImplForIM roomManager;

		@Autowired
		@Lazy
		private MessageService messageService;




		//单聊普通敏感词预警数量
		//private  int chatWarningKeywordNum = appConfig.getChatWarningKeywordNum();

		//单聊否词预警数量
		//private  int chatWarningnotKeywordNum = appConfig.getChatWarningNotwordNum();

		//群聊普通敏感词预警数量
		//private  int groupWarningKeywordNum = appConfig.getGroupWarningKeywordNum();

		//群聊否词预警数量
		//private  int groupWarningNotKeywordNum = appConfig.getGroupWarningNotwordNum();

		@Override
	public void onMessage(String message) {

		log.info("[敏感词检测入口] 接收到敏感词检测消息 - 原始消息: {}", message);

		try {
			KeywordDenyRecord keywordDenyRecord = new KeywordDenyRecord();

			JSONObject messageObj = JSON.parseObject(message);
			Integer fromUserId = messageObj.getInteger("fromUserId");
			String content = messageObj.getString("content");
			String fromUserName = messageObj.getString("fromUserName");
			
			log.info("[敏感词检测入口] 解析消息完成 - 发送者ID: {}, 发送者姓名: {}, 消息内容长度: {}", 
					fromUserId, fromUserName, content != null ? content.length() : 0);
			log.debug("[敏感词检测入口] 消息内容: {}", content);
			
			//系统消息不做处理
			if( 10000 == fromUserId || null != messageObj.getShort("imSys") ){
				log.info("[敏感词检测入口] 系统消息跳过处理 - 发送者ID: {}, imSys标识: {}", 
						fromUserId, messageObj.getShort("imSys"));
				return;
			}

			keywordDenyRecord.setMsgContent(content);
			keywordDenyRecord.setFromUserId(fromUserId);
			keywordDenyRecord.setFromUserName(fromUserName);
			keywordDenyRecord.setCreateTime(System.currentTimeMillis());
			
			log.debug("[敏感词检测入口] 构建敏感词记录对象完成 - 记录: {}", JSON.toJSONString(keywordDenyRecord));

			// 禁言检查
			User user = userCoreService.getUser(fromUserId);
			if(user == null) {
				log.warn("[敏感词检测入口] 用户不存在，跳过处理 - 用户ID: {}", fromUserId);
				return;
			}
			
			log.info("[敏感词检测入口] 开始进行敏感词检测 - 用户ID: {}, 消息内容长度: {}", fromUserId, content != null ? content.length() : 0);

            if(null != messageObj.getShort("chatType")){
                keywordDenyRecord.setChatType(1 == messageObj.getShort("chatType") ? (short) 1 : (short) 2);
            }else {
                keywordDenyRecord.setChatType(1 == messageObj.getShort("msgType") ? (short) 2 : (short) 1);
            }

			keywordDenyRecord.setMessageId(messageObj.getString("messageId"));
			keywordDenyRecord.setKeyword(messageObj.getString("keyword"));
			keywordDenyRecord.setKeywordType(messageObj.getShort("keywordType"));
			if (keywordDenyRecord.getChatType() == 2) { //群聊
				keywordDenyRecord.setRoomJid(messageObj.getString("toUserId"));
			} else {
				keywordDenyRecord.setToUserId(messageObj.getInteger("toUserId"));
			}
			keywordDenyRecord.setToUserName(messageObj.getString("toUserName"));

			try {
				//该消息id 已存在拦截记录，则不继续执行
				KeywordDenyRecord keywordDenyRecord1 = keywordDAO.findOne(KeywordDenyRecord.class, "messageId", keywordDenyRecord.getMessageId());
				if (null != keywordDenyRecord1) {
					return;
				}
				if(2==keywordDenyRecord.getChatType() && StringUtil.isEmpty(keywordDenyRecord.getToUserName())){
					Room room  = roomManager.getRoomByJid(keywordDenyRecord.getRoomJid());
					if(null!=room){
						keywordDenyRecord.setToUserName(room.getName());
					}
				}else{
					// 协议中无toUserName自行获取
					if(StringUtil.isEmpty(messageObj.getString("toUserName"))){
						keywordDenyRecord.setToUserName(userCoreService.getNickName(keywordDenyRecord.getToUserId()));
					}else{
						keywordDenyRecord.setToUserName(messageObj.getString("toUserName"));
					}
				}

				saveKeywordDenyRecord(keywordDenyRecord);

				String noticeTxt = (1 != keywordDenyRecord.getKeywordType() ? "敏感词截获":"否词拦截")+"通知:"+
						keywordDenyRecord.getFromUserName()+"("+keywordDenyRecord.getFromUserId()+")"+
						( 1==keywordDenyRecord.getChatType()  ? "给用户"+keywordDenyRecord.getToUserName()+"("+keywordDenyRecord.getToUserId()+")" :
								"给群组"+keywordDenyRecord.getToUserName()+"("+keywordDenyRecord.getRoomJid()+")" )
						+"发送了一条消息,含"+ (1 != keywordDenyRecord.getKeywordType() ? "敏感词":"否词")+" “"+keywordDenyRecord.getKeyword()+
						"” : "+keywordDenyRecord.getMsgContent();

				//通知管理后台默认好友
				noticeAdminDefultFriends(noticeTxt);

				sendRiskWarningMsgBegin(keywordDenyRecord);

				// 如果是否词，触发禁言功能
				if (keywordDenyRecord.getKeywordType() == 1) {
					log.info("检测到否词，准备对用户 {} 进行禁言处理，消息ID: {}, 否词内容: {}", 
						keywordDenyRecord.getFromUserId(), 
						keywordDenyRecord.getMessageId(),
						keywordDenyRecord.getKeyword());
					muteUserForKeyword(keywordDenyRecord);
				}

			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}

		} catch (Exception e) {
			log.error("[敏感词检测入口] 消息处理异常 - 原始消息: {}, 异常: {}", message, e.getMessage(), e);
			throw e;
		}

		}

		/**
		 * @Description:
		 */
		public void saveKeywordDenyRecord(KeywordDenyRecord keywordDenyRecord) {
			try {

				if (!StringUtil.isEmpty(keywordDenyRecord.getMessageId())) {
					//保存拦截记录
					keywordDAO.getDatastore().save(keywordDenyRecord);
				}

			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}

		}


		/**
		 * @Description: TODO(根据用户关键词拦截数量, 做封号操作)
		 */
		public void baseKeywordDenyRecordCountCloseUserAccount(int userId) {
			long denyRecordCount = keywordDAO.count("fromUserId", userId, "keywordDenyRecord");
		}


		/**
		 * 通知管理后台设置的默认好友用户
		 */
		public void noticeAdminDefultFriends(String keywordMsgContent) {

			List<Integer> defFriendUserList = userCoreService.queryDefFriendUserIdList();

			if (null != defFriendUserList && 0 < defFriendUserList.size()) {
				for (Integer defUserId : defFriendUserList) {
					adminManager.sendMsgToUser(defUserId, 1, keywordMsgContent);
				}
			}

		}

		/**
		 * 发送风险提示消息
		 */
		public void sendRiskWarningMsgBegin (KeywordDenyRecord keywordDenyRecord) {

			long denyRecordCount =keywordDAO.queryKeywordDenyRecordCountByType(keywordDenyRecord.getFromUserId(),keywordDenyRecord.getKeywordType(),keywordDenyRecord.getChatType());

			if(1==keywordDenyRecord.getKeywordType()){ //否词
				if(1==keywordDenyRecord.getChatType()&&denyRecordCount>=appConfig.getChatWarningNotwordNum()){
					/**
					 * 发消息提示当前聊天好友
					 */
					sendsendRiskWarningMsg(keywordDenyRecord);
				}else if(2==keywordDenyRecord.getChatType()&&denyRecordCount>=appConfig.getGroupWarningNotwordNum()) {
					/**
					 * 发消息提示当前聊天群组
					 */
					sendsendRiskWarningMsg(keywordDenyRecord);
				}

			}else {
				/**
				 * 普通敏感词
				 */
				if(1==keywordDenyRecord.getChatType()&&denyRecordCount>=appConfig.getChatWarningKeywordNum()){
					/**
					 * 发消息提示当前聊天好友
					 */
					sendsendRiskWarningMsg(keywordDenyRecord);
				}else if(2==keywordDenyRecord.getChatType()&&denyRecordCount>=appConfig.getGroupWarningKeywordNum())  {
					/**
					 * 发消息提示当前聊天群组
					 */
					sendsendRiskWarningMsg(keywordDenyRecord);
				}
			}


		}

		public void sendsendRiskWarningMsg(KeywordDenyRecord keywordDenyRecord){

			MessageBean messageBean=new MessageBean();
			messageBean.setContent("平台识别到用户 "+keywordDenyRecord.getFromUserName()+" 在发送一些包含违规内容的消息,请勿相信，谨防上当受骗");
			messageBean.setFromUserId(keywordDenyRecord.getFromUserId() + "");
			messageBean.setToUserId(keywordDenyRecord.getToUserId()+"");
			messageBean.setFromUserName(keywordDenyRecord.getFromUserName());
			messageBean.setType(MessageType.sensitiveWordsNotice);
			messageBean.setMessageId(com.basic.utils.StringUtil.randomUUID());
			if(1==keywordDenyRecord.getChatType()) {
				messageBean.setToUserId(keywordDenyRecord.getToUserId() + "");
				messageBean.setMsgType(0);// 单聊
				messageService.send(messageBean);
			}else {
				messageBean.setMsgType(1);// 群聊
				messageBean.setObjectId(keywordDenyRecord.getRoomJid());
				messageService.sendMsgToGroupByJid(keywordDenyRecord.getRoomJid(),messageBean);
			}
		}

		/**
		 * 对发送否词的用户进行禁言处理
		 */
		public void muteUserForKeyword(KeywordDenyRecord keywordDenyRecord) {
			try {
				log.info("开始执行禁言处理，用户ID: {}, 聊天类型: {}, 消息ID: {}", 
					keywordDenyRecord.getFromUserId(), 
					keywordDenyRecord.getChatType(), 
					keywordDenyRecord.getMessageId());
				
				// 获取系统配置中的禁言时间
				Config config = SKBeanUtils.getSystemConfig();
				int muteTime = config.getMuteTime();
				byte muteTimeUnit = config.getMuteTimeUnit();
				
				log.info("获取到系统禁言配置，禁言时长: {}, 时间单位: {}", muteTime, muteTimeUnit);
				
				if (muteTime <= 0) {
					log.info("系统未配置禁言时间，跳过禁言处理");
					return;
				}
				
				// 计算禁言截止时间（毫秒）
				long muteEndTime = calculateMuteEndTime(muteTime, muteTimeUnit);
				log.info("计算得到禁言截止时间: {}, 当前时间: {}", muteEndTime, System.currentTimeMillis());

				// 发送禁言通知消息给被禁言的用户
				sendMuteNotificationToUser(keywordDenyRecord.getFromUserId(), muteEndTime);
				
				//执行禁言，将禁言时间存储到数据库
				muteUserForPrivateChat(keywordDenyRecord.getFromUserId(), muteEndTime);

				/*
				// 如果是群聊消息，对用户在该群进行禁言
		//if (keywordDenyRecord.getChatType() == 2 && !StringUtil.isEmpty(keywordDenyRecord.getRoomJid())) {
		//	log.info("执行群聊禁言，群组JID: {}", keywordDenyRecord.getRoomJid());
	//		muteUserInRoom(keywordDenyRecord.getFromUserId(), keywordDenyRecord.getRoomJid(), muteEndTime);
	//	} else {
			// 如果是单聊消息，对用户进行全局单聊禁言
		//	log.info("执行单聊禁言");
		//	muteUserForPrivateChat(keywordDenyRecord.getFromUserId(), muteEndTime);
		}*/
		
		log.info("用户 {} 因发送否词被禁言，禁言截止时间: {}", keywordDenyRecord.getFromUserId(), muteEndTime);
				
			} catch (Exception e) {
				log.error("禁言处理失败: {}", e.getMessage(), e);
			}
		}
		
		/**
		 * 计算禁言截止时间
		 */
		private long calculateMuteEndTime(int muteTime, byte muteTimeUnit) {
			long currentTime = System.currentTimeMillis();
			long muteMillis = 0;
			
			log.info("计算禁言截止时间，禁言时长: {}, 时间单位: {}", muteTime, muteTimeUnit);
			
			switch (muteTimeUnit) {
				case 1: // 秒
					muteMillis = muteTime * 1000L;
					log.info("禁言时间单位为秒，转换为毫秒: {}", muteMillis);
					break;
				case 2: // 分
					muteMillis = muteTime * 60 * 1000L;
					log.info("禁言时间单位为分钟，转换为毫秒: {}", muteMillis);
					break;
				case 3: // 时
					muteMillis = muteTime * 60 * 60 * 1000L;
					log.info("禁言时间单位为小时，转换为毫秒: {}", muteMillis);
					break;
				case 4: // 天
					muteMillis = muteTime * 24 * 60 * 60 * 1000L;
					log.info("禁言时间单位为天，转换为毫秒: {}", muteMillis);
					break;
				default:
					muteMillis = muteTime * 60 * 60 * 1000L; // 默认按小时计算
					log.info("禁言时间单位未知，默认按小时计算，转换为毫秒: {}", muteMillis);
			}
			
			long endTime = currentTime + muteMillis;
			log.info("禁言开始时间: {}, 禁言时长(毫秒): {}, 禁言截止时间: {}", 
				currentTime, muteMillis, endTime);
			
			return endTime;
		}
		
		/**
		 * 对用户在指定群组进行禁言
		 */
		private void muteUserInRoom(Integer userId, String roomJid, long muteEndTime) {
			try {
				log.info("开始在群组中禁言用户，用户ID: {}, 群组JID: {}, 禁言截止时间: {}", userId, roomJid, muteEndTime);
				// 获取群组信息
				Room room = roomManager.getRoomByJid(roomJid);
				if (room == null) {
					log.warn("群组不存在: {}, 无法执行禁言", roomJid);
					return;
				}
				log.info("成功获取群组信息，群组ID: {}, 群组名称: {}", room.getId(), room.getName());
				
				// 获取群成员信息
				Room.Member member = roomManager.getMember(room.getId(), userId);
				if (member == null) {
					log.warn("用户 {} 不在群组 {} 中，无法执行禁言", userId, roomJid);
					return;
				}
				log.info("成功获取群成员信息，用户角色: {}, 当前禁言状态: {}", member.getRole(), member.getTalkTime());
				
				// 设置禁言时间
				long oldTalkTime = member.getTalkTime();
				member.setTalkTime(muteEndTime);
				log.info("设置群成员禁言时间，原禁言时间: {}, 新禁言时间: {}", oldTalkTime, muteEndTime);
				
				// 更新群成员信息
				User adminUser = userCoreService.getUser(10000);
				if (adminUser == null) {
					log.warn("无法获取管理员用户(10000)，禁言操作可能无效");
				}
				log.info("准备更新群成员信息，管理员ID: {}, 群组ID: {}", adminUser != null ? adminUser.getUserId() : "null", room.getId());
				roomManager.updateMember(adminUser, room.getId(), member, 0, 0);
				
				// 验证禁言是否生效
				Room.Member updatedMember = roomManager.getMember(room.getId(), userId);
				if (updatedMember != null) {
					log.info("禁言操作后群成员状态，禁言时间: {}, 是否成功: {}", 
						updatedMember.getTalkTime(), 
						updatedMember.getTalkTime() >= muteEndTime);
				} else {
					log.warn("无法验证禁言是否生效，更新后无法获取群成员信息");
				}
				
				log.info("成功对用户 {} 在群组 {} 进行禁言，截止时间: {}", userId, roomJid, muteEndTime);
				
			} catch (Exception e) {
			log.error("群组禁言失败: {}, 详细错误: {}", e.getMessage(), e);
		}
	}

	/**
	 * 对用户进行单聊禁言
	 */
	private void muteUserForPrivateChat(Integer userId, long muteEndTime) {
		boolean isSuccess = false; // 定义成功标志变量
		try {
			log.info("开始执行单聊禁言，用户ID: {}, 禁言截止时间: {}", userId, muteEndTime);
			// 获取用户信息
			User user = userCoreService.getUser(userId);
			if (user == null) {
				log.error("单聊禁言失败：无法获取用户信息，用户ID: {}", userId);
				return;
			}
			
			log.info("成功获取用户信息，用户名: {}, 当前禁言状态: {}", user.getNickname(), user.getTalkTime());
			
			// 记录更新前的详细状态
			log.info("=== 禁言更新操作开始 ===");
			log.info("用户ID: {}, 用户名: {}", userId, user.getNickname());
			log.info("更新前用户状态 - 禁言时间: {}, 当前时间: {}", user.getTalkTime(), System.currentTimeMillis());
			log.info("目标禁言截止时间: {}, 禁言时长: {}毫秒", muteEndTime, (muteEndTime - System.currentTimeMillis()));
			
			// 设置用户禁言时间
			user.setTalkTime(muteEndTime);
			log.info("设置用户禁言时间，原禁言时间: {}, 新禁言时间: {}", user.getTalkTime(), muteEndTime);
			
			// 更新用户信息
			log.info("准备更新用户信息到数据库");
			
			// 使用Map方式更新talkTime字段
			Map<String, Object> updateMap = new HashMap<>();
			updateMap.put("talkTime", muteEndTime);
			log.info("使用Map方式更新talkTime字段，值为: {}", muteEndTime);
			log.info("更新参数详情 - userId: {}, updateMap: {}", userId, updateMap);
			
			// 记录数据库连接状态
			try {
				log.info("检查UserCoreService状态: {}", userCoreService != null ? "正常" : "为空");
				if (userCoreService != null && userCoreService.getUserDao() != null) {
					log.info("UserDao状态: 正常");
				} else {
					log.error("UserDao状态: 异常或为空");
				}
			} catch (Exception e) {
				log.error("检查服务状态时发生异常: {}", e.getMessage());
			}
			
			try {
				// 记录更新操作开始时间
				long updateStartTime = System.currentTimeMillis();
				log.info("开始执行数据库更新操作，时间戳: {}", updateStartTime);
				
				// 调用更新方法并检查结果  更新禁言时间
				userCoreService.updateTalkTime(userId, muteEndTime);
				
				long updateEndTime = System.currentTimeMillis();
				log.info("数据库更新操作完成，耗时: {}毫秒", (updateEndTime - updateStartTime));
				
				// 等待一小段时间确保数据库操作完成
				Thread.sleep(100);
				
				// 验证更新结果 - 重新查询用户信息
				log.info("开始验证更新结果，重新查询用户信息...");
				User updatedUser = userCoreService.getUser(userId);
				
				if (updatedUser != null) {
					log.info("=== 更新结果验证 ===");
					log.info("更新后用户信息 - 用户ID: {}, 用户名: {}", userId, updatedUser.getNickname());
					log.info("更新后禁言时间: {}, 期望禁言时间: {}", updatedUser.getTalkTime(), muteEndTime);
					log.info("时间差异: {}毫秒", Math.abs(updatedUser.getTalkTime() - muteEndTime));
					log.info("当前时间: {}, 是否仍在禁言期: {}", System.currentTimeMillis(), updatedUser.getTalkTime() > System.currentTimeMillis());
					
					if (updatedUser.getTalkTime() >= muteEndTime) {
						log.info("✅ 禁言设置成功！数据库中的禁言时间({})已更新为期望值({})", updatedUser.getTalkTime(), muteEndTime);
						isSuccess = true;
						
						// 发送禁言通知消息给被禁言的用户
						// sendMuteNotificationToUser(userId, muteEndTime);
						
					} else {
						log.error("❌ 禁言设置失败！数据库中的禁言时间({})小于期望的禁言时间({})", updatedUser.getTalkTime(), muteEndTime);
						log.error("可能的原因: 1.数据库更新失败 2.字段映射错误 3.事务回滚 4.缓存问题");
						isSuccess = false;
					}
				} else {
					log.error("❌ 更新后无法获取用户信息，用户ID: {}", userId);
					log.error("可能的原因: 1.用户被删除 2.数据库连接问题 3.查询异常");
					isSuccess = false;
				}
				
				// 额外验证：直接查询数据库
				try {
					log.info("=== 额外验证：直接查询数据库 ===");
					User directQueryUser = userCoreService.getUserDao().get(userId);
					if (directQueryUser != null) {
						log.info("直接查询结果 - 禁言时间: {}, 与期望值差异: {}毫秒", 
							directQueryUser.getTalkTime(), Math.abs(directQueryUser.getTalkTime() - muteEndTime));
					} else {
						log.error("直接查询用户信息失败");
					}
				} catch (Exception directQueryEx) {
					log.error("直接查询数据库时发生异常: {}", directQueryEx.getMessage(), directQueryEx);
				}
				
			} catch (InterruptedException ie) {
				log.error("线程等待被中断: {}", ie.getMessage());
				Thread.currentThread().interrupt();
				isSuccess = false;
			} catch (Exception e) {
				log.error("❌ 更新用户禁言时间时发生异常，用户ID: {}", userId);
				log.error("异常类型: {}, 异常信息: {}", e.getClass().getSimpleName(), e.getMessage());
				log.error("异常堆栈:", e);
				isSuccess = false;
			}
			
			log.info("=== 禁言更新操作结束，最终结果: {} ===", isSuccess ? "成功" : "失败");

		} catch (Exception e) {
			log.error("单聊禁言失败: {}, 详细错误: {}", e.getMessage(), e);
		}
	}
	
	/**
	 * 发送禁言通知消息给被禁言的用户
	 */
	private void sendMuteNotificationToUser(Integer userId, long muteEndTime) {
		try {
			log.info("开始发送禁言通知，用户ID: {}, 禁言截止时间: {}", userId, muteEndTime);
			
			// 获取用户信息
			User user = userCoreService.getUser(userId);
			if (user == null) {
				log.warn("无法获取用户信息，跳过发送禁言通知，用户ID: {}", userId);
				return;
			}
			
			// 发送消息
			MessageBean messageBean = new MessageBean();
			messageBean.setType(MessageType.KEEP_SILENT);//敏感词触发禁言

			// messageBean.setObjectId(roomId.toString());
			//messageBean.setObjectId(room.getJid());
			messageBean.setFromUserId(userId + "");
			messageBean.setFromUserName(user.getNickname());
			messageBean.setToUserId(userId + "");
			messageBean.setToUserName(user.getNickname());
			messageBean.setContent(muteEndTime + "");
			messageBean.setMessageId(StringUtil.randomUUID());
			
			messageService.send(messageBean);
			
			log.info("成功发送禁言通知，用户ID: {}", userId);
			
		} catch (Exception e) {
			log.error("发送禁言通知失败，用户ID: {}, 错误: {}", userId, e.getMessage(), e);
		}
	}

}
