package com.basic.im.model;

import com.basic.im.comm.model.MessageBean;
import com.chat.imclient.BaseClientHandler;
import com.chat.imclient.BaseClientListener;
import com.chat.imclient.BaseIMClient;
import com.chat.imserver.common.message.ChatMessage;
import com.chat.imserver.common.message.MessageHead;
import com.chat.imserver.common.packets.ChatType;
import com.chat.imserver.common.utils.StringUtils;
import com.basic.utils.StringUtil;
import org.tio.client.ClientChannelContext;
import org.tio.client.ClientTioConfig;
import org.tio.client.ReconnConf;
import org.tio.client.TioClient;
import org.tio.utils.thread.pool.DefaultThreadFactory;
import org.tio.utils.thread.pool.SynThreadPoolExecutor;

import java.util.concurrent.*;

/**
 *
 * @author tanyaowu
 */
public class IMPushClient extends BaseIMClient{

	private static ThreadPoolExecutor groupExecutor=null;

	private static SynThreadPoolExecutor tioExecutor;

	private static ReconnConf reconnConf=null;
	static {
		getTioExecutor();
		getGroupExecutor();
		//reconnConf = new ReconnConf(5000L);
	}


	/**
	 *
	 * @param ip  IM 服务器Ip
	 * @param port IM 服务器端口
	 * @param clientHandler 客户端消息处理监听
	 * @param clientListener 客户端事件处理监听
	 */
    @Override
	public void initIMClient(String ip,int port,BaseClientHandler clientHandler,BaseClientListener clientListener) {

		try {
			setExecutor((ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1, DefaultThreadFactory.getInstance("pressure-client-pool")));
			/*clientHandler.setImClient(this);
			clientListener.setImClient(this);
			setPingTime(80000);*/
			super.initIMClient(ip, port,clientHandler,clientListener);
		} catch (Exception e) {
			log.error(e.toString(), e);
		}
	}
	
	public void sendMessage(MessageBean messageBean) {
		ChatMessage message=new ChatMessage();
		MessageHead messageHead=new MessageHead();
		messageHead.setFrom("10005");

		byte chatType=ChatType.CHAT;
		if(0==messageBean.getMsgType()){
            if(StringUtil.isEmpty(messageBean.getTo()))
                messageHead.setTo(messageBean.getToUserId());
            else
                messageHead.setTo(messageBean.getTo());
        }else if(1==messageBean.getMsgType()) {
			chatType=ChatType.GROUPCHAT;
			messageHead.setTo(messageBean.getRoomJid());
		}else if(2==messageBean.getMsgType()) {
			chatType=ChatType.ALL;
		}
		
		messageHead.setChatType(chatType);
		messageHead.setMessageId(StringUtils.newStanzaId());
		
		message.setContent(messageBean.getContent().toString());
		message.setFromUserId(messageBean.getFromUserId());
		message.setFromUserName(messageBean.getFromUserName());
		message.setToUserId(messageBean.getToUserId());
		message.setToUserName(messageBean.getToUserName());
		message.setType((short)messageBean.getType());
		message.setObjectId(messageBean.getObjectId().toString());
		message.setFileName(messageBean.getFileName());
		message.setTimeSend(System.currentTimeMillis());
		message.setMessageHead(messageHead);
		sendMessage(message);
		
	}

	/**
	 *
	 * @return
	 * @author tanyaowu
	 */
	public static synchronized ThreadPoolExecutor getGroupExecutor() {

			LinkedBlockingQueue<Runnable> groupQueue = new LinkedBlockingQueue<>();
			//			ArrayBlockingQueue<Runnable> groupQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
			String groupThreadName = "chat-im-group";
			DefaultThreadFactory defaultThreadFactory = DefaultThreadFactory.getInstance(groupThreadName);

			groupExecutor = new ThreadPoolExecutor(1, Runtime.getRuntime().availableProcessors(), 10, TimeUnit.SECONDS, groupQueue, defaultThreadFactory);

			groupExecutor.prestartCoreThread();
			return groupExecutor;
	}

	/**
	 *
	 * @return
	 * @author tanyaowu
	 */
	public static synchronized SynThreadPoolExecutor getTioExecutor() {

			LinkedBlockingQueue<Runnable> tioQueue = new LinkedBlockingQueue<>();
			String tioThreadName = "chat-im-worker";
			DefaultThreadFactory defaultThreadFactory = DefaultThreadFactory.getInstance(tioThreadName, Thread.MAX_PRIORITY);

			tioExecutor = new SynThreadPoolExecutor(1, Runtime.getRuntime().availableProcessors(), 10, tioQueue, defaultThreadFactory, tioThreadName);

			tioExecutor.prestartCoreThread();
			return tioExecutor;
	}

	public ClientTioConfig getClientGroupContext() {
		/*ClientGroupContext clientGroupContext = new ClientGroupContext(this.clientHandler, this.clientListener, reconnConf,tioExecutor,groupExecutor);

		return clientGroupContext;*/
		return getClientTioConfig();
	}
	@Override
	public ClientTioConfig getClientTioConfig() {
		ClientTioConfig clientGroupContext = new ClientTioConfig(this.clientHandler, this.clientListener, reconnConf);

		return clientGroupContext;
	}
	@Override
	public ClientChannelContext getClientChannelContext() throws Exception {
		ClientChannelContext clientChannel;
		try {
			ClientTioConfig context=null;
			if(null==tioClient){
				context=getClientGroupContext();
				context.setHeartbeatTimeout(pingTime);
				tioClient=new TioClient(context);

			}
			clientChannel =tioClient.connect(serverNode);
			clientChannel.tioConfig=context;


		} catch (Exception e) {
			throw e;
		}
		if(null==clientChannel)
			log.error("getClientChannelContext  connect Server fail ====>");
		return clientChannel;

	}


}
