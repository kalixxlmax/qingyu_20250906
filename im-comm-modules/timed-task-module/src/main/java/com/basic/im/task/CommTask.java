package com.basic.im.task;

import com.basic.common.model.PageResult;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.common.service.RedPacketsManager;
import com.basic.im.common.service.SkTransferManager;
import com.basic.im.entity.SysApiLog;
import com.basic.im.live.dao.LiveRoomDao;
import com.basic.im.live.entity.LiveRoom;
import com.basic.im.live.service.impl.LiveRoomManagerImpl;
import com.basic.im.message.IMessageRepository;
import com.basic.im.room.service.RoomManager;
import com.basic.im.user.dao.impl.UserDaoImpl;
import com.basic.im.user.dao.impl.UserStatusCountDao;
import com.basic.im.user.entity.UserStatusCount;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
@Slf4j
@Component
@EnableScheduling
@EnableAsync
public class CommTask {
	//implements ApplicationListener<ApplicationContextEvent>
	
	@Autowired
	private UserStatusCountDao userStatusCountDao;
	@Autowired
	private UserDaoImpl userDao;

	@Autowired
	private LiveRoomDao liveRoomDao;

	@Autowired(required = false)
	private RedPacketsManager redPacketsManager;

	@Autowired(required = false)
	private SkTransferManager transferManager;
	

	@Autowired
	@Lazy
	private IMessageRepository messageRepository;


	@Autowired
	private LiveRoomManagerImpl liveRoomManager;

	@Autowired
	private UserCoreRedisRepository userCoreRedisRepository;

	@Autowired
	private RoomManager roomManager;

/*	@Autowired(required=false)
	private AppConfig appConfig;
	@Resource(name = TaskManagementConfigUtils.SCHEDULED_ANNOTATION_PROCESSOR_BEAN_NAME)
	private ScheduledAnnotationBeanPostProcessor scheduledProcessor;*/
	 public CommTask() {
			super();
	 }
	 
	/*@Override
	public void onApplicationEvent(ApplicationContextEvent event) {
		 if(event.getApplicationContext().getParent() != null)
			 return;
		 //root application context 没有parent，他就是老大.
		 //需要执行的逻辑代码，当spring容器初始化完成后就会执行该方法。
		
				
				if(0==appConfig.getOpenTask()){

						ThreadUtils.executeInThread((Callback) obj -> {
							 try {
									Thread.sleep(10000);
									scheduledProcessor.destroy();
									log.info("====定时任务被关闭了=======》");
								 } catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
						});

				   }else log.info("====定时任务开启中=======》");
	}*/

	//	@Scheduled(cron = "0 */1 * * * ?")
	/**
	 * 半小时执行一次的定时任务
	 */
	@Async("taskExecutor")
	@Scheduled(cron = "0 0/30 * * * ?")
	public void executeHalfAnHourTask(){
        /**
         * 过期的单聊聊天记录
         */
		messageRepository.deleteTimeOutChatMsgRecord();

		/**
		 * 过期的群聊聊天记录
		 */
		//messageRepository.deleteOutTimeMucMsg();
	}

	 /**
	 *	一个小时执行   一次的定时任务 0/1
	 */
	@Async("taskExecutor")
	@Scheduled(cron = "0 0 0/1 * * ?")
	public void executeHourTask() {

		long start = System.currentTimeMillis();

		// 刷新红包
		autoRefreshRedPackect();
		log.info("刷新红包成功,耗时" + (System.currentTimeMillis() - start) + "毫秒");

		// 刷新转账
		autoRefreshTransfer();
		log.info("刷新转账成功,耗时" + (System.currentTimeMillis() - start) + "毫秒");

		refreshUserStatusHour();
	}


	 /**
	 每天执行一次定时任务     每天凌晨4:00定时删除 0 0 0 * *
	 */
	@Async("taskExecutor")
	@Scheduled(cron = "0 0 4 * * ?")
	public void executeDayTask(){
		
		//refreshUserStatus();

		messageRepository.deleteOutTimeMucMsg();

		messageRepository.deleteMucHistory();
		
		//删除系统日志
		deleteSysLogs();
		messageRepository.mucMsgDBCreateIndexs();

		roomManager.deleteDealLine();
		
	}
	/**
	5分钟 采集一次用户在线状态统计
	 */
	@Async("taskExecutor")
	@Scheduled(cron = "0 0/5 * * * ?")
 	public void refreshUserStatusCount(){
		long count=userCoreRedisRepository.getAllOnlineUserCount();
		UserStatusCount userCount=new UserStatusCount();
		userCount.setType(1);
		userCount.setCount(count);
		userCount.setTime(DateUtil.currentTimeSeconds());
		userStatusCountDao.addUserStatusCount(userCount);
		log.info("刷新用户状态统计======》 {}" ,count);
	}


	/**
	一个小时 采集一次用户在线状态统计
	 */
	//@Scheduled(cron = "0 0 0/1 * * ?")
 	public void refreshUserStatusHour(){
		
		long currentTime = DateUtil.currentTimeSeconds();
		long startTime=currentTime- KConstants.Expire.HOUR;
			
		long endTime=currentTime;

			System.out.println("当前时间:"+DateUtil.TimeToStr(new Date()));
			UserStatusCount userStatusCount = userStatusCountDao.getUserStatusCount(startTime,endTime,1);
			if(null!=userStatusCount) {
				UserStatusCount uCount = new UserStatusCount();
				uCount.setTime(startTime);
				uCount.setType(2);
				uCount.setCount(userStatusCount.getCount());
				userStatusCountDao.addUserStatusCount(uCount);
				log.info("最高在线用户======》  {}",uCount.getCount());
			}
				
	}


	/**
	 将所有用户在线状态   设为不在线
	 */
	//@Scheduled(cron = "0 0 4 * * ?")
	public void refreshUserStatus(){

		userDao.updateUserOnline();
	}



	/**
	 执行  更新 昨天的 用户在线状态统计 情况
	 */
	@Async("taskExecutor")
	@Scheduled(cron = "0 0 2 * * ?")
 	public void refreshUserStatusDay(){
		Date yesterday=DateUtil.getYesterdayMorning();
		long startTime=yesterday.getTime()/1000;
		long endTime=startTime+KConstants.Expire.DAY1;
		log.info("Day_Count 当前时间:"+ DateUtil.TimeToStr(new Date()));
		UserStatusCount userStatusCount = userStatusCountDao.getUserStatusCount(startTime,endTime,1);
		if(null!=userStatusCount) {
			UserStatusCount uCount=new UserStatusCount();
			uCount.setTime(startTime);
			uCount.setType(3);
			uCount.setCount(userStatusCount.getCount());
			userStatusCountDao.addUserStatusCount(uCount);
			log.info("Day_Count 最高在线用户======》   {}",uCount.getCount());
		}
	}

	
	
	/** 
	* @Description:（每天凌晨定时清除十五天前的系统日志） 
	**/ 
	public void deleteSysLogs(){
		long beginTime = DateUtil.getOnedayNextDay(DateUtil.currentTimeSeconds(), 15, 1);

		Query query = userDao.createQuery();
		query.addCriteria(Criteria.where("time").lte(beginTime));
		log.info("累积清除   "+ com.basic.im.comm.utils.DateUtil.strToDateTime(beginTime)+"  前的  "+ userDao.getDatastore().count(query, SysApiLog.class) +"  条系统日志记录");
		userDao.getDatastore().remove(query,SysApiLog.class);
	}

	// 定时清除直播间
	//@Scheduled(cron = "0 0 0 0/7 * ?")
	public void clearLiveRoom(){
		PageResult<LiveRoom> pageResult = liveRoomDao.getLiveRoomList(DateUtil.currentTimeSeconds()-(KConstants.Expire.DAY7));
		log.info("=========定时删除直播间========  "+pageResult.getCount());
		for(LiveRoom liveRoom:pageResult.getData()){
			liveRoomManager.deleteLiveRoom(liveRoom.getRoomId());
		}
	}
	
	
	//红包超时未领取 退回余额
	private void autoRefreshRedPackect(){
		if(null==redPacketsManager){
			return;
		}
		redPacketsManager.autoRefreshRedPackect();
		
	}
	


	// 转账超时未领取 退回余额
	public void autoRefreshTransfer(){
		if(null==transferManager){
			return;
		}
		transferManager.autoRefreshTransfer();
	}



}
