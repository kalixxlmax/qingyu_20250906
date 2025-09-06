package com.basic.im.live.service.impl;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.basic.common.model.PageResult;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.common.service.PaymentManager;
import com.basic.im.dao.GiftDao;
import com.basic.im.dao.GiveGiftDao;
import com.basic.im.entity.Config;
import com.basic.im.entity.Gift;
import com.basic.im.entity.Givegift;
import com.basic.im.event.MemberExitLiveRoomEvent;
import com.basic.im.live.dao.BlackDao;
import com.basic.im.live.dao.LiveRoomDao;
import com.basic.im.live.dao.LiveRoomMemberDao;
import com.basic.im.live.entity.Black;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.live.entity.LiveRoom.LiveRoomMember;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.pay.entity.BaseConsumeRecord;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.support.Callback;
import com.basic.im.user.constants.MoneyLogConstants.MoenyAddEnum;
import com.basic.im.user.constants.MoneyLogConstants.MoneyLogEnum;
import com.basic.im.user.constants.MoneyLogConstants.MoneyLogTypeEnum;
import com.basic.im.user.entity.User;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.user.service.RoleCoreService;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.SKBeanUtils;
import com.basic.redisson.ex.LockFailException;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class LiveRoomManagerImpl {

	//余额不足
	static final int InsufficientBalance = 104001;
	
	public final  String mucMsg="mucmsg_";
	@Autowired
    private LiveRoomDao liveRoomDao;
	public LiveRoomDao getLiveRoomDao(){
		return liveRoomDao;
	}

	@Autowired
    private LiveRoomMemberDao liveRoomMemberDao;

	@Autowired
    private BlackDao blackDao;
    @Autowired
    private GiftDao giftDao;
    @Autowired
    private GiveGiftDao giveGiftDao;

    @Autowired(required=false)
//	@Lazy
	private PaymentManager paymentManager;

	@Autowired
	@Lazy
	private MessageService messageService;

	@Autowired
	@Lazy
	private IMessageRepository messageRepository;

	@Autowired
	private UserCoreService userCoreService;

	@Autowired
	private RoomManagerImplForIM roomManager;

	@Autowired
	private RoleCoreService roleCoreService;


	private  UserCoreService getUserManager(){
		return userCoreService;
	};

	public LiveRoom queryLiveRoom(ObjectId roomId){
		String liveUrl = SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl();

		LiveRoom ownLiveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null == ownLiveRoom) {
			return null;
		}
		// 返回时加上完整地址
		if(!ownLiveRoom.getUrl().startsWith("rtmp://")){
			ownLiveRoom.setUrl(liveUrl+ownLiveRoom.getUrl());
		}
		return ownLiveRoom;
	}


	public LiveRoom queryLiveRoomByLiveRoomId(String roomId){
		String liveUrl = SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl();

		LiveRoom ownLiveRoom = liveRoomDao.getLiveRoomByLiveRoomId(roomId);
		if(null == ownLiveRoom) {
			return null;
		}
		// 返回时加上完整地址
		if(!ownLiveRoom.getUrl().startsWith("rtmp://")){
			ownLiveRoom.setUrl(liveUrl+ownLiveRoom.getUrl());
		}
		return ownLiveRoom;
	}

	public LiveRoom getLiveRoom(Integer userId,String roomId){
		String liveUrl = SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl();
		if(!StringUtil.isEmpty(roomId)){
			LiveRoom liveIngRoom = liveRoomDao.getLiveRoomByLiveRoomId(roomId);
			if(null != liveIngRoom){
				// 可以给直播人的当前信息
//					throw new ServiceException(KConstants.ResultCode.BeingLiveByRoom);
				return liveIngRoom;
			}else{
				// 群内没人直播
				LiveRoom liveRoom = liveRoomDao.getLiveRoom(new ObjectId(roomId));
				if(null == liveRoom) {
					liveRoom=liveRoomDao.getLiveRoomByUserId(userId);
					if(null!=liveIngRoom) {
						return liveRoom;
					}
                } else{
					return liveRoom;
				}
			}
		}
		LiveRoom ownLiveRoom = liveRoomDao.getLiveRoomByUserId(userId);
		if(null == ownLiveRoom) {
            return null;
        }
		// 返回时加上完整地址
		if(!ownLiveRoom.getUrl().startsWith("rtmp://")){
			ownLiveRoom.setUrl(liveUrl+ownLiveRoom.getUrl());
		}
        return ownLiveRoom;
	}

	public List<LiveRoom> queryLiveRoomList(Integer userId,Integer pageIndex,Integer pageSize){
		String liveUrl = SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl();

		List<LiveRoom> liveRoomList = liveRoomDao.queryLiveRoomList(userId,pageIndex,pageSize);
		if(liveRoomList.isEmpty()) {
			return liveRoomList;
		}
		for (LiveRoom liveRoom : liveRoomList) {
			// 返回时加上完整地址
			if(!liveRoom.getUrl().startsWith("rtmp://")){
				liveRoom.setUrl(liveUrl+liveRoom.getUrl());
			}
		}

		return liveRoomList;
	}

	public LiveRoom getLiveRoomByJid(String jid){
		LiveRoom liveRoom = liveRoomDao.getLiveRoomByJid(jid);
		// 返回时加上完整地址
		if(null!=liveRoom&&!liveRoom.getUrl().startsWith("rtmp://")) {
            liveRoom.setUrl(SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl()+liveRoom.getUrl());
        }
		return liveRoom;
	}


	public PageResult<Document> findLiveRoomListByUserId(String userId, int pageIndex, int pageSize){
		PageResult<Document> documentPageResult = liveRoomDao.findLiveRoomListByUserId(userId, pageIndex, pageSize);
		documentPageResult.getData().stream().forEach((val)->{
			Object userId_ = val.get("_id");
			val.append("nickname",userCoreService.getNickName(Integer.valueOf(userId_.toString())));
		});
		return documentPageResult;
	}
	
	//创建直播间
	public LiveRoom createLiveRoom(LiveRoom room){
        LiveRoom liveRoom = liveRoomDao.getLiveingByUserId(room.getUserId());
		if(null != liveRoom) {
            throw new ServiceException(KConstants.ResultCode.OtherDevicesliveIng);
        }
		// 群组直播创建直播间
		if(!StringUtil.isEmpty(room.getLiveRoomId())) {
			// 一个群组同时只能有一个用户直播
			LiveRoom liveIngRoom = liveRoomDao.getLiveRoomByLiveRoomId(room.getLiveRoomId());
			if (null != liveIngRoom) {
                throw new ServiceException(KConstants.ResultCode.BeingLiveByRoom);
            }
		}
		User user = getUserManager().getUser(room.getUserId());
		room.setNickName(user.getNickname());
		room.setCreateTime(DateUtil.currentTimeSeconds());
		room.setNotice(room.getNotice());
		room.setNumbers(1);
		if(StringUtil.isEmpty(room.getJid())){
			String jid =StringUtil.randomUUID();
					messageService.createMucRoomToIMServer(jid,user.getPassword(), user.getUserId().toString(),
					room.getName());
			room.setJid(jid);
		}
		String liveUrl = SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl();
		room.setUrl(liveUrl+room.getUserId()+"_"+DateUtil.currentTimeSeconds());
		room.setRoomId(ObjectId.get());
		liveRoomDao.addLiveRoomReturn(room);
		LiveRoom.LiveRoomMember member=new LiveRoom.LiveRoomMember();
		member.setUserId(room.getUserId());
		member.setCreateTime(DateUtil.currentTimeSeconds());
		member.setNickName(getUserManager().getUser(room.getUserId()).getNickname());
		member.setType(1);
		member.setRoomId(room.getRoomId());
		liveRoomMemberDao.addLiveRoomMember(member);
		if(!StringUtil.isEmpty(room.getLiveRoomId())){
			room.setUrl(SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl()+roomManager.getRoom(new ObjectId(room.getLiveRoomId())).getId());
		}
		log.info("=== createLiveRoom ===  liveRoom {}  ， liveRoomId {}",JSON.toJSONString(room),room.getLiveRoomId());
		return room;
	}
	//修改直播间信息
	public void updateLiveRoom(Integer userId,LiveRoom room){
		LiveRoomMember liveRoomMember = getLiveRoomMember(room.getRoomId(), userId);
		if(null == liveRoomMember || 1 != liveRoomMember.getType()) {
            throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
        }
		Map<String,Object> map = new HashMap<>();
		if(!StringUtil.isEmpty(room.getName()) || !StringUtil.isEmpty(room.getNotice())){
			if(!StringUtil.isEmpty(room.getName())) {
                map.put("name", room.getName());
            }
			if(!StringUtil.isEmpty(room.getNotice())) {
                map.put("notice", room.getNotice());
            }
			updateNoticeOrName(userId,room.getRoomId());
		}
		if(!StringUtil.isEmpty(room.getUrl())) {
            map.put("url", room.getUrl());
        }
		/*if(!StringUtil.isEmpty(room.getNotice()))
		    map.put("notice", room.getNotice());*/
        map.put("currentState", room.getCurrentState());
		liveRoomDao.updateLiveRoom(room.getRoomId(),userId,map);
	}
	
	//删除直播间
	public void deleteLiveRoom(ObjectId roomId){
        LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null !=liveRoom){
			// 删除聊天记录
			messageRepository.deleteMucMsg(liveRoom.getJid());
			//删除直播间中的成员
            liveRoomMemberDao.deleteLiveRoomMember(roomId);
			
			//删除群组离线消息记录
			messageRepository.dropRoomChatHistory(liveRoom.getJid());
			// 删除直播间
            liveRoomDao.deleteLiveRoom(roomId);
		}
		
	}

	@Autowired
	private LiveRoomRedisRepository liveRoomRedisRepository;
	//开始/结束直播
	public void start(ObjectId roomId,int status,String liveRoomId,boolean isEnterInto) {
		Integer userId = ReqUtil.getUserId();
		try {
			LiveRoom liveRooming = liveRoomDao.getLiveingByUserId(userId);
			if(null != liveRooming&&!roomId.equals(liveRooming.getRoomId())) {
				throw new ServiceException(KConstants.ResultCode.OtherDevicesliveIng);
			}
			liveRoomRedisRepository.executeOnLock(liveRoomRedisRepository.getLiveStart(liveRoomId),0, 30,o -> {
				LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
				VerifyUtil.isRollback(null == liveRoom,KConstants.ResultCode.NotInLiveRoom);
				VerifyUtil.isRollback(1 == liveRoom.getCurrentState(),KConstants.ResultCode.LiveRoomLock);
				// 群组 ID 不为空时验证
				if (1==status&&StrUtil.isNotBlank(liveRoomId)){
					LiveRoom liveRoomByLiveRoomId = liveRoomDao.getLiveRoomByLiveRoomId(liveRoomId);
					if(liveRoomByLiveRoomId != null &&!liveRoomByLiveRoomId.getUserId().equals(userId)){
						VerifyUtil.isRollback(liveRoomRedisRepository.getLiveStartFlag(liveRoomId), KConstants.ResultCode.BeingLiveByRoom);
					}
				}
				Map<String,Object> map = new HashMap<>();
				map.put("status", status);
				if(1==status){
					liveRoomRedisRepository.setLiveStartFlag(liveRoomId,1L);
					map.put("startTime",DateUtil.currentTimeSeconds());
					map.put("liveRoomId",liveRoomId);
				}else {
					map.put("endTime",DateUtil.currentTimeSeconds());

				}
				if(!StringUtil.isEmpty(liveRoomId) && 1 == status&&1!=liveRoom.getStatus()){
					ObjectId groupId = new ObjectId(liveRoomId);
					// 权限校验
					Room room = roomManager.getRoom(groupId);
					if(null == room) {
						throw new  ServiceException(KConstants.ResultCode.ParamsAuthFail);
					} else if(-1 == room.getS()){
						throw new  ServiceException(KConstants.ResultCode.RoomIsLock);
					}else {
						if(0 == room.getAllowOpenLive()){
							Room.Member member = roomManager.getMember(groupId, liveRoom.getUserId());
							if(member.getRole() > 2) {
								throw new  ServiceException(KConstants.ResultCode.BanMemberOpenLive);
							}
						}
					}
					// 维护群组直播状态
					String nickName = liveRoom.getNickName();
					Map<String,Object> roomMap = Maps.newConcurrentMap();
					roomMap.put("liveStatus",1);
					roomMap.put("liveUserId",liveRoom.getUserId());
					roomMap.put("liveUserName",nickName);

					roomMap.put("startTime",DateUtil.currentTimeSeconds());
					roomManager.startOrCloseRoomLive(roomManager.getRoom(groupId),roomMap,MessageType.OpenLiveRoom,nickName);
					// 直播间绑定群组

				}
				liveRoomDao.updateLiveRoom(roomId,0,map);
				if(0==status){
					liveRoomRedisRepository.setLiveStartFlag(liveRoomId,0L);
				}

				return true;
			});
		} catch (LockFailException e) {
			if (isEnterInto){
				int i = 3; // 重试次数
				while (i > 0){
					try {
						Thread.sleep(50); // 等待用户创建直播间
					} catch (InterruptedException interruptedException) {
						interruptedException.printStackTrace();
					}
					if (!liveRoomRedisRepository.getLiveStartFlag(liveRoomId)){
						// 直播间已经创建成功，直接加入
						enterIntoLiveRoom(userId,roomId);
						return;
					}
					i--;
				}
			}
			VerifyUtil.isRollback(true,KConstants.ResultCode.Failure);
		}catch (InterruptedException e){
			e.printStackTrace();
		}
	}
	
	//后台查询所有房间
	public PageResult<LiveRoom> findConsoleLiveRoomList(String name, String nickName, Integer userId, Integer page, Integer limit, int status, int type){
		PageResult<LiveRoom> result=new PageResult<LiveRoom>();
        result = liveRoomDao.findLiveRoomList(name,nickName,userId,status,page,limit,type);
		for (LiveRoom liveRoom : result.getData()) {
			if(!liveRoom.getUrl().contains("//")){
				liveRoom.setUrl(SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl()+liveRoom.getUrl());
			}
		}
		return result;
	}
	
	//查询所有房间
	public List<LiveRoom> findLiveRoomList(String name,String nickName,Integer userId,Integer page, Integer limit,int status,int type){
        PageResult<LiveRoom> pageResult = liveRoomDao.findLiveRoomList(name,nickName,userId,status,page,limit,type);
        /*List<LiveRoom> result = new ArrayList<>();
		for (LiveRoom liveRoom : pageResult.getData()) {
			if (userCoreService.getOnlinestateByUserId(liveRoom.getUserId()) == KConstants.ONE){
				if(!liveRoom.getUrl().contains("//")){
					liveRoom.setUrl(SKBeanUtils.getImCoreService().getClientConfig().getLiveUrl()+liveRoom.getUrl());
				}
				result.add(liveRoom);
			}else{
				liveRoomDao.updateLiveRoomStatus(liveRoom.getRoomId(),KConstants.ZERO); // 用户离线，修改为下线状态
			}
		}*/
		return pageResult.getData();
	}
	
	//加入直播间
	public void enterIntoLiveRoom(Integer userId,ObjectId roomId){
        LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
        if(null==liveRoom){
			throw new ServiceException(KConstants.ResultCode.DataNotExists);
		}
		// 不能加入自己已经在直播的直播间
		/*if(userId.equals(liveRoom.getUserId()) && 1 == liveRoom.getStatus()) {
			throw new ServiceException(KConstants.ResultCode.OtherDevicesliveIng);
		}*/
		if(!userId.equals(liveRoom.getUserId()) && 0 == liveRoom.getStatus()) {
            throw new ServiceException(KConstants.ResultCode.LiveRoomNotStart);
        }
        Black black = blackDao.getBlack(roomId,userId);
		//成员是否在黑名单
		if(null == black){
			LiveRoomMember members = liveRoomMemberDao.getLiveRoomMember(roomId, userId);
			if(null == members){
				LiveRoomMember member=new LiveRoomMember();
				member.setUserId(userId);
				member.setRoomId(roomId);
				member.setCreateTime(DateUtil.currentTimeSeconds());
				member.setNickName(getUserManager().getUser(userId).getNickname());
				member.setType(userId.equals(liveRoom.getUserId())? 1 : 3);
				liveRoomMemberDao.addLiveRoomMember(member);
				liveRoomDao.updateLiveRoomNum(roomId,1);
			}else{
				Map<String,Object> map = new HashMap<>();
				map.put("online", 1);
				liveRoomMemberDao.updateLiveRoomMember(roomId,userId,map);
			}



			User user=getUserManager().getUser(userId);
			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.JOINLIVE);
			messageBean.setContent(liveRoom.getName());
			messageBean.setObjectId(liveRoom.getJid());
			messageBean.setFileName(liveRoom.getRoomId().toString());
			messageBean.setToUserId(user.getUserId()+"");
			messageBean.setToUserName(user.getNickname());
			messageBean.setMessageId(StringUtil.randomUUID());
			// 发送群聊
			messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
		}else{
			throw new ServiceException(KConstants.ResultCode.KickedLiveRoom);
		}
	}


	//后台退出直播间
	public void exitLiveRoom(Integer userId,ObjectId roomId,String liveRoomId,Integer closeUserId,Boolean isThrow){
		//删除直播间成员
        LiveRoomMember liveRoomMember=liveRoomMemberDao.getLiveRoomMember(roomId,userId);
		if(null == liveRoomMember) {
			if (isThrow){
				throw new ServiceException(KConstants.ResultCode.UserNotInLiveRoom);
			}
        }else{
			User user = getUserManager().getUser((null != closeUserId && closeUserId > 0) ? closeUserId : userId);
			LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
			if (liveRoom!=null){
				liveRoomMemberDao.deleteLiveRoomMember(liveRoom.getRoomId(),userId);
				// 修改直播间总人数
				if(liveRoom.getNumbers()<=0){
					return;
				}else{
					liveRoomDao.updateLiveRoomNum(roomId,-1);
				}
				if(liveRoomMember.getType()==1){
					blackDao.deleteBlack(roomId); // 删除黑名单
					// 主播关闭直播间
					if(!StringUtil.isEmpty(liveRoomId)){
						// 维护群组中的直播间状态，直播的用户,修改直播间状态
						liveRoomDao.updateLiveRoomStatus(liveRoom.getRoomId(),0);
						updateRoomAndLiveRoom(roomId,liveRoomId,user);
					}
				}

				ThreadUtils.executeInThread(callback ->{
					MessageBean messageBean=new MessageBean();
					messageBean.setType(MessageType.LiveRoomSignOut);
					messageBean.setObjectId(liveRoom.getJid());
					messageBean.setFromUserId(user.getUserId()+"");
					messageBean.setFromUserName(user.getNickname());
					messageBean.setToUserId(user.getUserId()+"");
					messageBean.setToUserName(user.getNickname());
					messageBean.setMessageId(StringUtil.randomUUID());
					// 发送群聊通知
					messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
				});
			}
		}
	}


	/**
	 * 关闭、退出直播间事件回调
	 */
	@EventListener
	public void memberExitLiveRoomEvent(MemberExitLiveRoomEvent event){
		if (ObjectUtil.isNotEmpty(event.getRoomId()) && ObjectId.isValid(event.getRoomId())){
			LiveRoom liveRoom = liveRoomDao.getLiveRoomByLiveRoomId(event.getRoomId());
			if (liveRoom!=null){
				exitLiveRoom(event.getUserId(),liveRoom.getRoomId(),liveRoom.getLiveRoomId(),ReqUtil.getUserId(),false);
			}
		}
	}

	/**
	 * 维护群组中的直播间状态，直播的用户
	 */
	private void updateRoomAndLiveRoom(ObjectId roomId,String liveRoomId,User user){
		// 维护群组中的直播间状态，直播的用户
		Map<String,Object> roomMap = new HashMap<>();
		roomMap.put("liveUserId","");
		roomMap.put("liveUserName","");
		roomMap.put("liveStatus",0);
		// 发送关闭直播间通知到群组
		roomManager.startOrCloseRoomLive(roomManager.getRoom(new ObjectId(liveRoomId),user.getUserId()),roomMap,MessageType.CloseLiveRoom,user.getNickname());
		// 解除直播间关联的群组
		liveRoomDao.updateAttribute(roomId,"liveRoomId","");
	}

	// 用户离线后移出直播间
	public void OutTimeRemoveLiveRoom(Integer userId){
		List<LiveRoomMember> memberList=liveRoomMemberDao.queryLiveRoomMemberList(userId);
		for (LiveRoomMember liveRoomMember : memberList) {
			if(null == liveRoomMember) {
                continue;
            }
			LiveRoom liveRoom = liveRoomDao.getLiveRoom(liveRoomMember.getRoomId());
			if(null==liveRoom){
				liveRoomMemberDao.deleteLiveRoomMember(liveRoomMember.getRoomId(),userId);
				continue;
			}
			User user=getUserManager().getUser(userId);
			//主播
			if(1==liveRoom.getStatus()&&1==liveRoomMember.getType()){
				liveRoomDao.updateAttribute(liveRoom.getRoomId(),"status",0);
				blackDao.deleteBlack(liveRoom.getRoomId());
				// 处理群组直播
				if(!StringUtil.isEmpty(liveRoom.getLiveRoomId())){
					log.info("user offine userId : {} , roomId : {}, liveRoomId：{}",userId,liveRoom.getRoomId(),liveRoom.getLiveRoomId());
					updateRoomAndLiveRoom(liveRoom.getRoomId(),liveRoom.getLiveRoomId(),user);
				}
			}
			liveRoomMemberDao.deleteLiveRoomMember(liveRoom.getRoomId(),userId);
			//修改直播间总人数
			if(liveRoom.getNumbers()<=0){
				return;
			}else{
				liveRoomDao.updateLiveRoomNum(liveRoom.getRoomId(),-1);
			}

			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.RemoveLiveRoom);
			messageBean.setObjectId(liveRoom.getJid());
			messageBean.setFromUserId(user.getUserId()+"");
			messageBean.setFromUserName(user.getNickname());
			messageBean.setToUserId(user.getUserId()+"");
			messageBean.setToUserName(user.getNickname());
			messageBean.setMessageId(StringUtil.randomUUID());
			// 如果是创建者
			if(liveRoomMember.getType()==1){
				messageBean.setOther(userId.toString());
			}
			// 发送群聊通知
			messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
			messageBean.setMsgType(0);
			messageService.send(messageBean);
		}



	}

	//踢出直播间
	public void kick(Integer userId,ObjectId roomId){
		/*//删除直播间成员
		*/
		LiveRoomMember liveRoomMember = liveRoomMemberDao.getLiveRoomMember(roomId,userId);
		if(null == liveRoomMember){
			throw new ServiceException(KConstants.ResultCode.NotInLiveRoom);
		}
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		//修改直播间总人数
		if(liveRoom.getNumbers()<=0){
			return;
		}else{
            liveRoomDao.updateLiveRoomNum(roomId,-1);
		}
		User touser=getUserManager().getUser(userId);
		if(touser==null){
			return;
		}
		User user=getUserManager().getUser(ReqUtil.getUserId());
		
		MessageBean messageBean=new MessageBean();
		messageBean.setType(MessageType.LiveRoomSignOut);
		messageBean.setObjectId(liveRoom.getJid());
		messageBean.setFromUserId(user.getUserId()+"");
		messageBean.setFromUserName(user.getNickname());
		messageBean.setToUserId(touser.getUserId()+"");
		messageBean.setToUserName(touser.getNickname());
		messageBean.setMessageId(StringUtil.randomUUID());
		try {
			// 发送群聊通知
			messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
			ThreadUtils.executeInThread((Callback) obj ->
					liveRoomMemberDao.deleteLiveRoomMember(roomId,userId)
			);
		} catch (Exception e) {	
		}
		//添加到黑名单
		Black black=new Black();
		black.setRoomId(roomId);
		black.setUserId(userId);
		black.setTime(DateUtil.currentTimeSeconds());
        blackDao.addBlack(black);
	}
	
	//解锁、锁定直播间
	public void operationLiveRoom(int userId, int currentState) {
		List<LiveRoom> liveRooms = liveRoomDao.queryLiveRoomList(userId);
		liveRooms.stream().forEach(liveRoom -> {
			// 处理直播间的状态
			Map<String,Object> map = new HashMap<>();
			map.put("currentState", currentState);
			map.put("status", 0);// 关闭直播
			liveRoomDao.updateLiveRoom(liveRoom.getRoomId(),0,map);

			User user = getUserManager().getUser(liveRoom.getUserId());
			MessageBean messageBean = new MessageBean();
			messageBean.setType(MessageType.RoomDisable);
			messageBean.setObjectId(liveRoom.getJid());
			messageBean.setFromUserId(10005 + "");
			//messageBean.setFromUserName("Background system administrator");
			messageBean.setFromUserName("后台管理员");
			messageBean.setToUserId(user.getUserId() + "");
			messageBean.setToUserName(user.getNickname());
			messageBean.setMessageId(StringUtil.randomUUID());
			try {
				// 发送群聊通知
				messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}


	//解锁、锁定直播间
	public void operationLiveRoom(ObjectId liveRoomId, int currentState) {
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(liveRoomId);

		// 处理直播间的状态
		Map<String,Object> map = new HashMap<>();
		map.put("currentState", currentState);
		map.put("status", 0);// 关闭直播
		liveRoomDao.updateLiveRoom(liveRoom.getRoomId(),0,map);

		User user = getUserManager().getUser(liveRoom.getUserId());
		MessageBean messageBean = new MessageBean();
		messageBean.setType(MessageType.RoomDisable);
		messageBean.setObjectId(liveRoom.getJid());
		messageBean.setFromUserId(10005 + "");
		//messageBean.setFromUserName("Background system administrator");
		messageBean.setFromUserName("后台管理员");
		messageBean.setToUserId(user.getUserId() + "");
		messageBean.setToUserName(user.getNickname());
		messageBean.setMessageId(StringUtil.randomUUID());
		try {
			// 发送群聊通知
			messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}




	public void modifyRtmpUrl(ObjectId liveRoomId, String newRtmpUrl) {
		//更换直播间url
		Map<String,Object> map = new HashMap<>();
		map.put("url", newRtmpUrl);//更新rtmpUrl
		map.put("status", 1);//同时将状态置为直播中
		liveRoomDao.updateLiveRoom(liveRoomId,0,map);
	}


	//查询房间成员
	public List<LiveRoomMember> findLiveRoomMemberList(ObjectId roomId,Integer pageIndex,Integer pageSize){
       return liveRoomMemberDao.getLiveRoomMemberList(roomId,1,pageIndex,pageSize);
	}
	public List<Integer> findMembersUserIds(ObjectId roomId){
		List<Integer> userIds=null;
        userIds = liveRoomMemberDao.findMembersUserIds(roomId,1);
		return userIds;
	}
	
	//获取单个成员
	public LiveRoomMember getLiveRoomMember(ObjectId roomId,Integer userId){
        LiveRoomMember liveRoomMember=liveRoomMemberDao.getLiveRoomMember(roomId,userId);
		return liveRoomMember;
		
	}
	
	//禁言/取消禁言
	public LiveRoomMember shutup(int adminUserId,int state,Integer userId,ObjectId roomId,String talkTime){
        LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		LiveRoomMember livemember=new LiveRoomMember();
		livemember = liveRoomMemberDao.getLiveRoomMember(roomId,userId);
        if(null == livemember){
        	throw new ServiceException(KConstants.ResultCode.NotInLiveRoom);
		}
		//修改状态
        Map<String,Object> map = new HashMap<>();
        map.put("state", state);
        if(!StringUtil.isEmpty(talkTime)){
			map.put("talkTime",Long.valueOf(talkTime));
		}
        liveRoomMemberDao.updateLiveRoomMember(roomId,userId,map);
		//xmpp推送
		MessageBean messageBean=new MessageBean();
		messageBean.setType(MessageType.LiveRoomBannedSpeak);
		if(state==1){
			messageBean.setContent(talkTime);
		}else{
			messageBean.setContent(0);
		}
		messageBean.setFromUserId(String.valueOf(adminUserId));
		messageBean.setFromUserName(userCoreService.getNickName(adminUserId));
		messageBean.setObjectId(liveRoom.getJid());
		messageBean.setToUserId(livemember.getUserId()+"");
		messageBean.setToUserName(livemember.getNickName());
		messageBean.setMessageId(StringUtil.randomUUID());
		// 发送群聊通知
		messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
		return livemember;
	}
	//添加礼物
	public void addGift(String name,String photo,double price,int type){
		Gift gift=new Gift();
		gift.setName(name);
		gift.setPhoto(photo);
		gift.setPrice(price);
        giftDao.addGift(gift);

	}
	//删除礼物
	public void deleteGift(ObjectId giftId){
        Gift gift = giftDao.getGift(giftId);
		if(null == gift) {
            throw new ServiceException(KConstants.ResultCode.DataNotExists);
        }
        giftDao.deleteGift(giftId);
	}
	
	//后台查询所有的礼物
	public Map<String,Object> consolefindAllgift(String name,int pageIndex,int pageSize){
		Map<String,Object> giftMap = Maps.newConcurrentMap();
        Map<String,Object> map = giftDao.getGiftListMap(name,pageIndex,pageSize);
		giftMap.put("total", map.get("total"));
		giftMap.put("data", map.get("data"));
		return giftMap;
	}
	
	//查询所有的礼物
	public List<Gift> findAllgift(String name,int pageIndex,int pageSize){
        List<Gift> giftList = giftDao.getGiftList(name,pageIndex,pageSize);
		return giftList;
	}

	public Gift queryGift(ObjectId giftId){
		return giftDao.getGift(giftId);
	}

	
	//送礼物
	public synchronized ObjectId giveGift(Integer userId,Integer toUserId,ObjectId giftId,int count,Double price,ObjectId roomId){

		// 礼物对应的总价格
		Double totalMoney = price * count; 
		User user= userCoreService.getUser(userId);

		LiveRoom liveRoom= liveRoomDao.getLiveRoom(roomId);
		Double balance =getUserManager().getUserMoenyV1(userId);
		if(balance<price*count){
			throw new ServiceException(InsufficientBalance);
		}
			Givegift givegift=new Givegift();
			givegift.setUserId(userId);
			givegift.setToUserId(toUserId);
			givegift.setGiftId(giftId);
			givegift.setCount(count);
			givegift.setPrice(price*count);
			givegift.setTime(DateUtil.currentTimeSeconds());
			givegift.setId(ObjectId.get());

			/**
			 * 没有支付模块不扣取余额
			 */
		   if (paymentManager != null){
				giveGiftUserBalanceChange(userId,totalMoney,toUserId,givegift.getId().toString());
			   liveRoomMemberDao.addMemberRewardCount(roomId,userId,totalMoney);
			   liveRoomDao.addRewardCount(roomId,totalMoney);

			}

            giveGiftDao.addGiveGift(givegift);

			
			//xmpp推送消息
			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.GIFT);
			messageBean.setFromUserId(userId.toString());
			messageBean.setFromUserName(user.getNickname());
			messageBean.setObjectId(liveRoom.getJid());
			messageBean.setContent(giftId.toString());
			messageBean.setMessageId(StringUtil.randomUUID());
			// 发送群聊s
			messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);

			return giftId;

	}


	/**
	 *送礼物时 用户余额改变的操作
	 * @return
	 */
	private boolean giveGiftUserBalanceChange(int userId,double totalMoney,int toUserId,String businessId){

		try {
			/**
			 * 用户的余额更新日志
			 */
			UserMoneyLog userMoneyLog =new UserMoneyLog(userId,toUserId,businessId,totalMoney,
					MoenyAddEnum.MOENY_REDUCE, MoneyLogEnum.LIVE_GIVE_PAY, MoneyLogTypeEnum.NORMAL_PAY);
			Config config=SKBeanUtils.getSystemConfig();
			double giftRatio;
			if(null != config) {
				giftRatio = totalMoney*config.getGiftRatio();
			}else {
				giftRatio = totalMoney*0.50;
			}
			/**
			 * 主播的余额更新日志
			 */
			UserMoneyLog toUserMoneyLog =new UserMoneyLog(toUserId,userId,businessId,giftRatio,
					MoenyAddEnum.MOENY_ADD, MoneyLogEnum.LIVE_GIVE_PAY, MoneyLogTypeEnum.RECEIVE);
			//扣除用户的余额
			getUserManager().rechargeUserMoeny(userMoneyLog);
			//增加主播的余额
			getUserManager().rechargeUserMoeny(toUserMoneyLog);

			// 系统分成
			ThreadUtils.executeInThread(obj -> {
				String tradeNo=StringUtil.getOutTradeNo();
				//创建送礼物记录
				BaseConsumeRecord record=new BaseConsumeRecord();
				record.setUserId(userId);
				record.setToUserId(toUserId);
				record.setTradeNo(tradeNo);
				record.setMoney(totalMoney);
				record.setBusinessId(userMoneyLog.getBusinessId());
				record.setCurrentBalance(userMoneyLog.getEndMoeny());
				record.setStatus(KConstants.OrderStatus.END);
				record.setType(KConstants.ConsumeType.LIVE_GIVE);
				record.setChangeType(KConstants.MOENY_REDUCE);
				record.setPayType(KConstants.PayType.BALANCEAY); //余额支付
				//record.setDesc("Live gift delivery");
				record.setDesc("直播礼物送出");
				record.setTime(DateUtil.currentTimeSeconds());
				record.setOperationAmount(totalMoney-giftRatio);
				record.setServiceCharge(giftRatio);
				paymentManager.addConsumRecord(record);
				tradeNo=StringUtil.getOutTradeNo();
				//创建接受礼物记录
				BaseConsumeRecord recordTo=new BaseConsumeRecord();
				recordTo.setUserId(toUserId);
				recordTo.setToUserId(userId);
				recordTo.setTradeNo(tradeNo);
				recordTo.setMoney(giftRatio);
				recordTo.setCurrentBalance(toUserMoneyLog.getEndMoeny());
				recordTo.setBusinessId(toUserMoneyLog.getBusinessId());
				recordTo.setStatus(KConstants.OrderStatus.END);
				recordTo.setType(KConstants.ConsumeType.LIVE_RECEIVE);
				recordTo.setChangeType(KConstants.MOENY_ADD);
				recordTo.setPayType(KConstants.PayType.BALANCEAY); //余额支付
				//recordTo.setDesc("Live gift income");
				recordTo.setDesc("直播礼物收入");
				recordTo.setTime(DateUtil.currentTimeSeconds());
				recordTo.setServiceCharge(giftRatio);
				recordTo.setOperationAmount(totalMoney - giftRatio);
				paymentManager.addConsumRecord(recordTo);


			});
		} catch (Exception e) {
			log.error(e.getMessage(),e.getMessage());
			log.info(" 直播送礼物更新余额异常");
		}
		return true;
	}
	
	//主播收到礼物的列表
	public PageResult<Givegift> getGiftList(Integer userId,String startDate,String endDate,Integer page,Integer limit){
		PageResult<Givegift> result=new PageResult<Givegift>();
		double totalMoney = 0;
		Config config=SKBeanUtils.getSystemConfig();
        List<Givegift> giveGiftList = null;
		PageResult<Givegift> queryResult = giveGiftDao.getGivegift(userId,startDate,endDate,page,limit);
		giveGiftList = queryResult.getData();
		for(Givegift givegift : giveGiftList){
            LiveRoom liveRoom = liveRoomDao.getLiveRoomByUserId(givegift.getToUserId());
			Gift gift = giftDao.getGift(givegift.getGiftId());
			if (!ObjectUtil.isEmpty(gift)){
				givegift.setGiftName(gift.getName());
			}
            if(null!=liveRoom) {
				givegift.setLiveRoomName(liveRoom.getName());
			}
			givegift.setActualPrice(config.getGiftRatio()*givegift.getPrice());
			// 当前总收入
			totalMoney += givegift.getActualPrice();
			givegift.setUserName(getUserManager().getNickName(givegift.getUserId()));
			givegift.setToUserName(getUserManager().getNickName(givegift.getToUserId()));
		}
		result.setData(giveGiftList);
		result.setCount(queryResult.getCount());
		result.setTotal(totalMoney);
		return result;
		
	}
	
	//购买礼物的记录
	public List<Givegift> giftdeal(Integer userId,int pageIndex,int pageSize){
        List<Givegift> givegiftList = giveGiftDao.getGiveGiftList(userId,0,pageIndex,pageSize,0);
		return givegiftList;
	}
	//发送弹幕
	public ObjectId barrage(Integer userId,ObjectId roomId,String text){
		User user= getUserManager().getUser(userId);
        LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		ObjectId barrageId=null;

			Givegift givegift=new Givegift();
			givegift.setCount(1);
			givegift.setPrice(1.0);
			givegift.setUserId(userId);
			givegift.setTime(DateUtil.currentTimeSeconds());
			givegift.setId(ObjectId.get());

			giveGiftDao.addGiveGift(givegift);
			barrageId=givegift.getGiftId();//xmpp推送
			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.BARRAGE);
			messageBean.setFromUserId(userId.toString());
			messageBean.setFromUserName(user.getNickname());
			messageBean.setObjectId(liveRoom.getJid());
			messageBean.setContent(text);
			messageBean.setMessageId(StringUtil.randomUUID());

			// 发送群聊
			messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
			return barrageId;

		
	}
	//设置/取消管理员
	public void setmanage(Integer userId,int type,ObjectId roomId){
        LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(userId.equals(liveRoom.getUserId())) {
            throw new ServiceException(KConstants.ResultCode.NotSetAnchorIsAdmin);
        }
        LiveRoomMember liveRoomMember = liveRoomMemberDao.getLiveRoomMember(roomId,userId);
		if(null == liveRoomMember){
			throw new ServiceException(KConstants.ResultCode.NotInLiveRoom);
		}
		if(liveRoomMember.getType() == type) {
            throw new ServiceException(KConstants.ResultCode.NotRepeatOperation);
        }
        Map<String,Object> map = new HashMap<>();
        map.put("type", type);
        liveRoomMemberDao.updateLiveRoomMember(roomId,userId,map);
		//xmpp推送
		MessageBean messageBean=new MessageBean();
		messageBean.setType(MessageType.LiveRoomSettingAdmin);
		if(type==2){//1为设置管理员
			messageBean.setContent(1);
		}else{
			messageBean.setContent(0);
		}
		messageBean.setFromUserId(liveRoom.getUserId().toString());
		messageBean.setFromUserName(liveRoom.getNickName());
		messageBean.setToUserName(liveRoomMember.getNickName());
		messageBean.setToUserId(liveRoomMember.getUserId().toString());
		messageBean.setObjectId(liveRoom.getJid());
		messageBean.setMessageId(StringUtil.randomUUID());
		// 发送群聊通知
		messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);

	}

	//点赞
	public void addpraise(ObjectId roomId){
        LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		//xmpp消息
		MessageBean messageBean=new MessageBean();
		messageBean.setType(MessageType.LIVEPRAISE);
		messageBean.setObjectId(liveRoom.getJid());
		messageBean.setMessageId(StringUtil.randomUUID());
		// 发送群聊
		messageService.sendMsgToGroupByJid(liveRoom.getJid(), messageBean);
	}
	
	//定时清除直播间
	public void clearLiveRoom(){
        liveRoomDao.clearLiveRoom();
	}

	/**
	 * 群主、管理员关闭群组内直播间
	 * @param userId
	 * @param roomId
	 * @param liveRoomId
	 */
	public void closeRoomLiveByAdmin(String roomId,String liveRoomId,Integer userId){
		Room.Member member = roomManager.getMember(new ObjectId(liveRoomId), userId);
		if(null == member || member.getRole() > KConstants.Room_Role.ADMIN){
			throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
		}
		User user=getUserManager().getUser(userId);
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(new ObjectId(roomId));
		if(null == liveRoom) {
            throw new ServiceException(KConstants.ResultCode.ParamsAuthFail);
        }
		String jid = liveRoom.getJid();
		ThreadUtils.executeInThread(callback ->{
			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.CloseLiveRoomByAdmin);
			messageBean.setObjectId(jid);
			messageBean.setFromUserId(user.getUserId()+"");
			messageBean.setFromUserName(user.getNickname());
			messageBean.setContent(user.getNickname());
			messageBean.setToUserId(user.getUserId()+"");
			messageBean.setToUserName(user.getNickname());
			messageBean.setMessageId(StringUtil.randomUUID());
			// 发送群聊通知
			messageService.sendMsgToGroupByJid(jid, messageBean);
		});
	}

	/**
	 * 修改直播间的名称或公告
	 * @param roomId
	 * @param userId
	 */
	public void updateNoticeOrName(Integer userId,ObjectId roomId){
		User user=getUserManager().getUser(userId);
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null == liveRoom) {
            throw new ServiceException(KConstants.ResultCode.ParamsAuthFail);
        }
		String jid = liveRoom.getJid();
		ThreadUtils.executeInThread(callback ->{
			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.UpdateLiveRoomNameOrNotive);
			messageBean.setObjectId(jid);
			messageBean.setFromUserId(user.getUserId()+"");
			messageBean.setFromUserName(user.getNickname());
			messageBean.setContent(user.getNickname());
			messageBean.setToUserId(user.getUserId()+"");
			messageBean.setToUserName(user.getNickname());
			messageBean.setMessageId(StringUtil.randomUUID());
			// 发送群聊通知
			messageService.sendMsgToGroupByJid(jid, messageBean);
		});
	}

	/**
	 * 开始讲解商品
	 */
	public void startExplainProduct(int userId,ObjectId roomId,String productId){
		User user=getUserManager().getUser(userId);
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null == liveRoom) {
			throw new ServiceException(KConstants.ResultCode.ParamsAuthFail);
		}else if(userId!=liveRoom.getUserId()){
			throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
		}
		String jid = liveRoom.getJid();
		ThreadUtils.executeInThread(callback ->{
			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.START_EXPLAIN_PRODUCT);
			messageBean.setObjectId(jid);
			messageBean.setFromUserId(user.getUserId()+"");
			messageBean.setFromUserName(user.getNickname());
			messageBean.setContent(productId);
			messageBean.setToUserId(jid);
			messageBean.setToUserName(user.getNickname());
			messageBean.setMessageId(StringUtil.randomUUID());
			// 发送群聊通知
			messageService.sendMsgToGroupByJid(jid, messageBean);
		});
		liveRoomDao.updateProductId(roomId,productId);
	}
	public void endExplainProduct(int userId,ObjectId roomId,String productId){
		User user=getUserManager().getUser(userId);
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null == liveRoom) {
			throw new ServiceException(KConstants.ResultCode.ParamsAuthFail);
		}else if(userId!=liveRoom.getUserId()){
			throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
		}
		String jid = liveRoom.getJid();
		ThreadUtils.executeInThread(callback ->{
			MessageBean messageBean=new MessageBean();
			messageBean.setType(MessageType.END_EXPLAIN_PRODUCT);
			messageBean.setObjectId(jid);
			messageBean.setFromUserId(user.getUserId()+"");
			messageBean.setFromUserName(user.getNickname());
			messageBean.setContent(productId);
			messageBean.setToUserId(jid);
			messageBean.setToUserName(user.getNickname());
			messageBean.setMessageId(StringUtil.randomUUID());
			// 发送群聊通知
			messageService.sendMsgToGroupByJid(jid, messageBean);
		});
		liveRoomDao.updateProductId(roomId,null);
	}

	public void importProducts(int userId, ObjectId roomId, List<String> productIdList) {

		LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null == liveRoom) {
			throw new ServiceException(KConstants.ResultCode.ParamsAuthFail);
		}else if(userId!=liveRoom.getUserId()){
			throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
		}
		if(null==liveRoom.getProductIdList()){
			liveRoom.setProductIdList(new ArrayList<>());
			liveRoom.getProductIdList().addAll(productIdList);
		}else {
			for (String str : productIdList) {
				if(!liveRoom.getProductIdList().contains(str)){
					liveRoom.getProductIdList().add(str);
				}
			}

		}
		liveRoomDao.updateProductIdList(userId,roomId,liveRoom.getProductIdList());


	}

	public void deleteProducts(int userId, ObjectId roomId, List<String> productIdList) {
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null == liveRoom) {
			throw new ServiceException(KConstants.ResultCode.ParamsAuthFail);
		}else if(userId!=liveRoom.getUserId()){
			throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
		}
		liveRoom.getProductIdList().removeAll(productIdList);
		liveRoomDao.updateProductIdList(userId,roomId,liveRoom.getProductIdList());
	}

	public void stickProduct(int userId, ObjectId roomId, String productId) {
		LiveRoom liveRoom = liveRoomDao.getLiveRoom(roomId);
		if(null == liveRoom) {
			throw new ServiceException(KConstants.ResultCode.ParamsAuthFail);
		}else if(userId!=liveRoom.getUserId()){
			throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
		}
		List<String> productIdList = liveRoom.getProductIdList();
		productIdList.remove(productId);
		productIdList.add(0,productId);
		liveRoomDao.updateProductIdList(userId,roomId,productIdList);

	}

	public void clickProduct(Integer userId, ObjectId roomId) {

		liveRoomDao.addClickProductCount(userId,roomId);
	}

	public void updateOrderStatistics(ObjectId roomId, int orderNum, double orderMoney) {
		liveRoomDao.updateOrderStatistics(roomId,orderNum,orderMoney);
	}

	public void share(Integer userId, ObjectId objectId) {
		liveRoomDao.addShareCount(objectId);
	}

	public PageResult<Gift> findGiftList(String name, int pageIndex, int pageSize){
		return giftDao.findGiftList(name, pageIndex, pageSize);
	}

	public Integer liveCount(){
		return liveRoomDao.liveCount();
	}
}
