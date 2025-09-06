package com.basic.im.service;

import com.mongodb.client.MongoCollection;
import com.basic.common.model.PageResult;
import com.basic.im.config.XMPPConfig;
import com.basic.im.entity.StickDialog;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.dao.TigaseMsgDao;
import com.basic.im.repository.MongoOperator;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Tigase 相关的管理类
 */
@Service(MessageRepository.BEAN_ID)
public class MessageRepository  extends IMessageRepository {

	public static final String BEAN_ID = "TigaseManagerImpl";

	@Autowired(required = false)
	private TigaseMsgDao tigaseMsgDao;

	@Autowired
	private XMPPConfig xmppConfig;



		@Override
	  public  void updateMsgIsReadStatus(int userId,String msgId){
			if (null == msgId) {
				return;
			}
		  tigaseMsgDao.updateMsgIsReadStatus(userId,msgId);

	  }
	
	@Override
	public void deleteLastMsg(String userId, String jid){
		tigaseMsgDao.deleteLastMsg(userId,jid);
	}

	/**
	 * 获取单聊消息数量
	 */
	@Override
	public long getMsgCountNum() {
		return tigaseMsgDao.getMsgCountNum();
	}
	

	/**
	 * 单聊消息数量统计      时间单位  每日、每月、每分钟、每小时
	 * @param counType  统计类型   1: 每个月的数据      2:每天的数据       3.每小时数据   4.每分钟的数据 (小时)
	 */
	@Override
	public List<Object> getChatMsgCount(String startDate, String endDate, short counType){
		return tigaseMsgDao.getChatMsgCount(startDate,endDate,counType);
	}
	

	/**
	 * 群聊消息数量统计      时间单位  每日、每月、每分钟、每小时
	 * @param counType  统计类型   1: 每个月的数据      2:每天的数据       3.每小时数据   4.每分钟的数据 (小时)
	 */
	@Override
	public List<Object> getGroupMsgCount(String roomId, String startDate, String endDate, short counType){
		return tigaseMsgDao.getGroupMsgCount(roomId,startDate,endDate,counType);
	}

	@Override
	public void cleanUserFriendHistoryMsg(int userId){
		tigaseMsgDao.cleanUserFriendHistoryMsg(userId,xmppConfig.getServerName());
	}


	@Override
	public void destroyUserMsgRecord(int userId){
		tigaseMsgDao.destroyUserMsgRecord(userId);

	}

	@Override
	public Document emojiMsg(int userId,String roomJid, String messageId){
		return tigaseMsgDao.queryCollectMessage(userId, roomJid, messageId);
	}

	@Override
	public List<Document> queryMsgDocument(int userId, String roomJid, List<String> messageIds){
		return tigaseMsgDao.queryMsgDocument(userId, roomJid, messageIds);
	}

	@Override
	public void deleteTigaseUser(int userId){
		tigaseMsgDao.cleanUserFriendHistoryMsg(userId,null);
	}

	@Override
	public void registerSystemNo(String userId, String password)  {


	}

	@Override
	public void examineTigaseUser(String userId, String password){




	}

	@Override
	public void registerAndXmppVersion(String userId, String password){

	}

	@Override
	public void updateToTig(String userId, String password) {

	}


	private byte[] generateId(String username) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		return md.digest(username.getBytes());
	}

	List<Document> queryChatMessageRecord(int userId,int receiver,long startTime,
							 long endTime,int pageIndex,
							 int pageSize, int maxType){
		return tigaseMsgDao.queryChatMessageRecord(userId,receiver,startTime,endTime,pageIndex,pageSize,maxType);
	}


	/**
	 * 好友之间的聊天记录
	 */
	@Override
	public PageResult<Document> queryFirendMsgRecord(Integer sender, Integer receiver, Integer page, Integer limit){
		PageResult<Document> result=tigaseMsgDao.queryFirendMsgRecord(sender,receiver,page,limit);

		return result;
	}

	/**
	 * 删除好友间的聊天记录
	 **/
	@Override
	public void delFriendsChatRecord(String... messageIds){
		MongoCollection<Document> dbCollection = tigaseMsgDao.getDatastore().getCollection("chat_msgs");
		Document query = new Document();
		query.put("messageId", new Document(MongoOperator.IN,messageIds));
		dbCollection.deleteMany(query);
	}

	private void saveFansCount(int userId) {
		/*BasicDBObject q = new BasicDBObject("_id", userId);
		DBCollection dbCollection = SKBeanUtils.getTigaseDatastore().getDB().getCollection("chat_msgs_count");
		if (0 == dbCollection.count(q)) {
			BasicDBObject jo = new BasicDBObject("_id", userId);
			jo.put("count", 0);// 消息数
			jo.put("fansCount", 1);// 粉丝数
			dbCollection.insert(jo);
		} else {
			dbCollection.update(q, new BasicDBObject("$inc", new BasicDBObject("fansCount", 1)));
		}*/
	}




	/**
	 定时删除  过期的单聊聊天记录
	 */
	@Override
	public void deleteTimeOutChatMsgRecord(){
		tigaseMsgDao.deleteTimeOutChatMsgRecord();

	}

	@Override
	public void deleteMucMsg(String roomJid){
		dropRoomChatHistory(roomJid);
	}
	/**
	 删除过期的 群组聊天消息
	 */
	@Override
	public void deleteOutTimeMucMsg() {
		tigaseMsgDao.deleteOutTimeMucMsg();
	}

	/**
	 删除 tigase 超过100条的聊天历史记录
	 */
	@Override
	public void deleteMucHistory() {
		tigaseMsgDao.deleteMucHistory();
		//log.info("timeCount  ---> "+(System.currentTimeMillis()-start));
	}

	@Override
	public void mucMsgDBCreateIndexs(){
		tigaseMsgDao.mucMsgDBCreateIndexs();
	}

	@Override
	public void dropRoomChatHistory(String roomJid){
		tigaseMsgDao.dropRoomChatHistory(roomJid);
	}


	@Override
	public List<Document> queryLastChatList(String userId, long startTime, long endTime, int pageSize, List<String> roomJidList, int needId, List<StickDialog> stickDialogs){
		return tigaseMsgDao.queryLastChatList(userId,startTime,endTime,pageSize,roomJidList,needId, stickDialogs);
	}

	@Override
	public void changePassword(String userId, String password, String newPwd) {
		try {
			String user_id = userId + "@" + xmppConfig.getServerName();

			Document q = new Document();
			q.put("_id", generateId(user_id));
			Document o = new Document();
			o.put("$set", new Document("password",newPwd));
			tigaseMsgDao.getDatastore().getCollection("tig_users").updateOne(q, o);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
