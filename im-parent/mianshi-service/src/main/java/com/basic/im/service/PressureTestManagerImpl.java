package com.basic.im.service;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.comm.utils.NumberUtil;
import com.basic.im.message.IMessageRepository;
import com.basic.im.model.IMPushClient;
import com.basic.im.model.IMPushConfig;
import com.basic.im.model.PressureParam;
import com.basic.im.model.PressureThread;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.dao.UserCoreDao;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.vo.JSONMessage;
import com.chat.imclient.BaseClientHandler;
import com.chat.imclient.BaseClientListener;
import com.chat.imclient.BaseIMClient;
import com.chat.imserver.common.message.AuthMessage;
import com.chat.imserver.common.message.MessageHead;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.utils.thread.pool.DefaultThreadFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** @version:（1.0） 
* @ClassName	PressureTest
* @Description: （压力测试） 
* @date:2018年11月13日下午5:46:54  
*/
@Slf4j
@Service
public class PressureTestManagerImpl {
	
	@Autowired
	private UserCoreDao userCoreDao;

	@Autowired
	@Lazy
	private RoomManagerImplForIM roomManager;

	@Autowired
	private UserManagerImpl userManager;

	@Autowired
	private IMessageRepository messageRepository;

	@Autowired(required = false)
	protected IMPushConfig imPushConfig;


	private Map<Integer, IMPushClient> userClient = Maps.newConcurrentMap();


	private int runStatus=0;//任务 运行状态   0  无任务   1  运行中  
	

	

	
	
	/**
	 *	1. 群内生成1000个用户 
	 *  
	 *  2. 模拟四百个用户发送消息   ===》 机器人  不够自动创建
	 *  
	 *  3. 可以 发送总条数、每秒条数 
	 */
	
	
	/** @Description:（创建一定数量的机器人） 
	* @param checkNum
	* @param jids
	**/ 
	public void createRobot(int checkNum,List<String> jids,Integer adminUserId){
		// 筛选群内离线的人，不够checkNum则创建机器人
		for (String jid : jids) {
			List<Integer> offlineUsers = new ArrayList<Integer>();
			ObjectId roomId = roomManager.getRoomId(jid);
			List<Integer> memberIds = roomManager.getCommonMemberIdList(roomId);
			int createNum = (int) (checkNum - memberIds.size());
			if(createNum > 0){
				List<Integer> addRobots = userManager.addRobot(createNum, true, roomId, adminUserId,null);
				offlineUsers.addAll(memberIds);
				offlineUsers.addAll(addRobots);
				log.info("群："+jid+"   需要创建机器人的个数："+createNum);
			}else {
				/**
				 * 人数 大于 三倍 随机取
				 * 100 70 1.3
				 * 100
				 */
				for (int i = 0; i <checkNum; i++) {
					Integer num = NumberUtil.getRandomByMinAndMax(1, memberIds.size());
					while (offlineUsers.contains(num)) {
						num = NumberUtil.getRandomByMinAndMax(1, memberIds.size());
					}
					offlineUsers.add(memberIds.get(num-1));
					memberIds.remove(num);
				}
				/*if(3<=(memberIds.size()/checkNum)) {

				}else {
					offlineUsers.addAll(memberIds);
				}*/
			}
			// 创建每个用户的连接client
			createClient(offlineUsers);
		}
	}

	/**
	 * 创建client
	 */
	public void createClient(List<Integer> userIds){
		if(userIds.isEmpty())
			return;
		for (Integer userId : userIds) {
			IMPushClient client = getIMClient(userId+"");
			userClient.put(userId,client);
		}
	}

	/**
	 * 获取连接
	 */
	private IMPushClient getIMClient(String userId) {
		IMPushClient client = new IMPushClient();
		client.setUserId(userId);

		client.setPingTime(imPushConfig.getPingTime());

		BaseClientHandler clientHandler = new BaseClientHandler() {

			@Override
			public void handlerReceipt(String messageId) {
				System.out.println("handlerReceipt ===> " + messageId);
			}
		};
		clientHandler.setImClient(client);
		BaseClientListener clientListener = new BaseClientListener() {

			@Override
			public AuthMessage authUserMessage(ChannelContext channelContext, BaseIMClient client) {
				MessageHead messageHead = new MessageHead();

				messageHead.setChatType((byte) 1);
				channelContext.userid = userId;
				messageHead.setFrom(userId + "/Server");
				AuthMessage authMessage = new AuthMessage();
				authMessage.setToken(imPushConfig.getServerToken());
				authMessage.setMessageHead(messageHead);
				return authMessage;
			}
		};
		clientListener.setImClient(client);

		client.initIMClient(imPushConfig.getHost(), imPushConfig.getPort(), clientHandler, clientListener);

		return client;
	}


	public JSONMessage mucTest(PressureParam param, Integer adminUserId) {
		if (1 == runStatus) {
			log.error("已有压测 任务 运行中  请稍后 请求 。。。。。。");
			return null;
		}
		System.out.println("压力测试：" + " roomJids: " + JSONObject.toJSONString(param.getJids()) + " checkNum: " + param.getCheckNum() + " sendMsgNum: " + param.getSendMsgNum()
				+ "  消息时间间隔:" + param.getTimeInterval());
		closeConnection();
		runStatus = 1;
		param.setAtomic(new AtomicInteger(0));

		String format = new SimpleDateFormat("MM-dd HH:mm").format(System.currentTimeMillis());

		param.setTimeStr(format);
		createRobot(param.getCheckNum(), param.getJids(), adminUserId);

		List<PressureThread> threads = Collections.synchronizedList(new ArrayList<>());
		param.setSendAllCount(param.getJids().size()*param.getSendMsgNum());
		for (String jid : param.getJids()) {
			String roomName = roomManager.getRoomName(jid);
			threads.add(new PressureThread(jid, roomName, param, userClient));
		}



		param.setStartTime(System.currentTimeMillis());// 开始时间
		try {
			final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), DefaultThreadFactory.getInstance("pressure-pool"));
			threads.forEach(th -> {
				threadPool.scheduleAtFixedRate(th, 1000, param.getTimeInterval(), TimeUnit.MILLISECONDS);
			});
			PressureParam.PressureResult result = null;
			while (runStatus == 1) {
				if (param.getAtomic().get() >= param.getSendAllCount()) {
					try {
						result = new PressureParam.PressureResult();
						result.setTimeCount((System.currentTimeMillis() - param.getStartTime()) / 1000);
						result.setSendAllCount(param.getSendAllCount());
						result.setTimeStr(param.getTimeStr());
						log.info("任务执行完毕 ：" + JSONObject.toJSONString(param));
						runStatus = 2;
						for (IMPushClient imPushClient : userClient.values()) {
							try {
								Tio.close(imPushClient.getContext(),null,null,true,true);
								if(null!=imPushClient.getContext()) {
									imPushClient.getContext().tioConfig.setStopped(true);
								}
								//imPushClient.stop();
							} catch (Exception e) {
								e.printStackTrace();
							}
						}

						threadPool.shutdown();
						userClient.clear();
						runStatus = 0;
						return JSONMessage.success(result);

					} catch (Exception e) {
						e.printStackTrace();
						break;
					}

				} else {
					Thread.sleep(1000);
				}
			}
			return null;


		} catch (Exception e) {
			runStatus = 0;
			return JSONMessage.success(new PressureParam.PressureResult());
		}
	}

	private void closeConnection() {
		if(userClient.isEmpty())
			return;
		userClient = Maps.newConcurrentMap();
	}

}
