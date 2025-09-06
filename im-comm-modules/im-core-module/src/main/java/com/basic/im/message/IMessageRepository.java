package com.basic.im.message;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoCollection;
import com.basic.common.model.PageResult;
import com.basic.im.config.XMPPConfig;
import com.basic.im.entity.StickDialog;
import com.basic.im.message.dao.TigaseMsgDao;
import com.basic.im.repository.MongoOperator;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class IMessageRepository {

    @Autowired
    protected TigaseMsgDao tigaseMsgDao;



    public  void updateMsgIsReadStatus(int userId,String msgId){
        if(null==msgId) {
            return;
        }
        tigaseMsgDao.updateMsgIsReadStatus(userId,msgId);

    }

    public void deleteLastMsg(String userId,String jid){
        tigaseMsgDao.deleteLastMsg(userId,jid);
    }

    /**
     * 删除某个用户所有的最后一条聊天记录
     * **/
    public void deleteUserAllLastMsg(String userId){
        tigaseMsgDao.deleteUserAllLastMsg(userId);
    }


    /**
     * 获取单聊消息数量
     * @return
     */
    public long getMsgCountNum() {
        //BasicDBObject query = new BasicDBObject();
//		return getMsgRepostory().count();
        return tigaseMsgDao.getMsgCountNum();
    }




    /**
     * 单聊消息数量统计      时间单位  每日、每月、每分钟、每小时
     * @param startDate
     * @param endDate
     * @param counType  统计类型   1: 每个月的数据      2:每天的数据       3.每小时数据   4.每分钟的数据 (小时)
     */
    public List<Object> getChatMsgCount(String startDate, String endDate, short counType){

        return tigaseMsgDao.getChatMsgCount(startDate,startDate,counType);
    }




    /**
     * 群聊消息数量统计      时间单位  每日、每月、每分钟、每小时
     * @param startDate
     * @param endDate
     * @param counType  统计类型   1: 每个月的数据      2:每天的数据       3.每小时数据   4.每分钟的数据 (小时)
     */
    public List<Object> getGroupMsgCount(String roomId,String startDate, String endDate, short counType){

        return tigaseMsgDao.getGroupMsgCount(roomId,startDate,endDate,counType);

    }


    @Autowired(required=false)
    private XMPPConfig xmppConfig;

    public void cleanUserFriendHistoryMsg(int userId){
        tigaseMsgDao.cleanUserFriendHistoryMsg(userId,xmppConfig.getServerName());
    }


    public void destroyUserMsgRecord(int userId){
        tigaseMsgDao.destroyUserMsgRecord(userId);
    }


    public void destroyFriendMessage(int userId, int toUserId){
        tigaseMsgDao.destroyFriendMessage(userId,toUserId);
    }

    public void destroyRoomMessage(String... jids){
        tigaseMsgDao.destroyRoomMessage(jids);
    }
    public void deleteRoomMemberMessage(String jid,Integer userId){
        tigaseMsgDao.deleteRoomMemberMessage(jid,userId);
    }

    public void destroyAllSystemMessage(){
        tigaseMsgDao.destroyAllSystemMessage();
    }

    public Document emojiMsg(int userId,String roomJid, String messageId){
        return tigaseMsgDao.queryCollectMessage(userId, roomJid, messageId);
    }

    public List<Document> queryMsgDocument(int userId, String roomJid, List<String> messageIds){
        return tigaseMsgDao.queryMsgDocument(userId, roomJid, messageIds);
    }

    public abstract void deleteTigaseUser(int userId);

    public abstract void registerSystemNo(String userId, String password);

    public abstract void examineTigaseUser(String userId,String password);

    public abstract void registerAndXmppVersion(String userId, String password);

    public abstract void updateToTig(String userId, String password);


    private byte[] generateId(String username) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return md.digest(username.getBytes());
    }




    List<Document> queryChatMessageRecord(int userId,int receiver,long startTime,
                                          long endTime,int pageIndex,
                                          int pageSize, int maxType){
        return tigaseMsgDao.queryChatMessageRecord(userId,receiver,startTime,endTime,pageIndex,pageSize,maxType);
    }






    // 好友之间的聊天记录
    public PageResult<Document> queryFirendMsgRecord(Integer sender, Integer receiver, Integer page, Integer limit){
        PageResult<Document> result=tigaseMsgDao.queryFirendMsgRecord(sender,receiver,page,limit);

        return result;
    }

    /** @Description:（删除好友间的聊天记录）
     * @param messageIds
     **/
    public void delFriendsChatRecord(String... messageIds){
        MongoCollection<Document> dbCollection = tigaseMsgDao.getDatastore().getCollection("chat_msgs");
        Document query = new Document();
        List<String> messageList = new ArrayList<>(messageIds.length);
        Collections.addAll(messageList,messageIds);
        query.put("messageId", new Document(MongoOperator.IN,messageList));
        dbCollection.deleteMany(query);
    }

    public void delFriendsChatMsg(int userId,int toUserId){
        try {
            tigaseMsgDao.cleanFriendMessage(userId,toUserId,0);
        }catch (Exception e){
            e.printStackTrace();
        }

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
     定时删除  过期的单聊 聊天记录
     */
    public void deleteTimeOutChatMsgRecord(){
        tigaseMsgDao.deleteTimeOutChatMsgRecord();

    }


    public void deleteMucMsg(String roomJid){
        dropRoomChatHistory(roomJid);
    }


    /**
     删除过期的 群组聊天消息
     */
    public void deleteOutTimeMucMsg() {
        tigaseMsgDao.deleteOutTimeMucMsg();
    }
    /**
     删除 tigase 超过100条的聊天历史记录
     */
    public void deleteMucHistory() {
        tigaseMsgDao.deleteMucHistory();
        //log.info("timeCount  ---> "+(System.currentTimeMillis()-start));
    }

    public abstract void mucMsgDBCreateIndexs();

    public void dropRoomChatHistory(String roomJid){
        tigaseMsgDao.dropRoomChatHistory(roomJid);
    }
    public List<Document> queryLastChatList(String userId, long startTime, long endTime, int pageSize, List<String> roomJidList){
        return tigaseMsgDao.queryLastChatList(userId,startTime,endTime,pageSize,roomJidList);
    }
    public PageResult<Document> queryActiveGroupList(int pageIndex,int pageSize){
        return tigaseMsgDao.queryActiveGroupList(pageIndex,pageSize);
    }
    public List<Document> queryLastChatList(String userId, long startTime, long endTime, int pageSize, List<String> roomJidList, int needId, List<StickDialog> stickDialogs){
        return tigaseMsgDao.queryLastChatList(userId,startTime,endTime,pageSize,roomJidList, needId,stickDialogs);
    }

    public List<Document> pullMessageBySeqNos(long userId, long toUserId, Set<Long> seqNos){
        return tigaseMsgDao.pullMessageBySeqNos(userId,toUserId,seqNos);
    }
    public List<Document> pullGroupMessageBySeqNos(String roomJid, Set<Long> seqNoSets) {
        return tigaseMsgDao.pullGroupMessageBySeqNos(roomJid,seqNoSets);
    }

    public abstract void changePassword(String s, String password, String newPwd);


    public  List<Document> queryChatMessageBySeqNo(Integer userId, int toUserId, long startSeqNo, long endSeqNo, int pageIndex, int pageSize){
        return tigaseMsgDao.queryChatMessageBySeqNo(userId,toUserId,startSeqNo,endSeqNo,pageIndex,pageSize);
    }

    public List<Document> queryGroupMessageBySeqNo(String roomJid, long startSeqNo, long endSeqNo, int pageIndex, int pageSize) {
        return tigaseMsgDao.queryGroupMessageBySeqNo(roomJid,startSeqNo,endSeqNo,pageIndex,pageSize);
    }


    public List<Document> queryMultipointChat(Integer userId, long startTime, long endTime, String from, String to, int pageSize){
        return tigaseMsgDao.queryMultipointChat(userId,startTime,endTime,from,to,pageSize);
    }


    public JSONObject queryChatDetails(Integer userId, List<Integer> userIds, List<String> roomIds){
        return tigaseMsgDao.queryChatDetails(userId,userIds,roomIds);
    }

}
