package com.basic.im.message.dao;

import com.alibaba.fastjson.JSONObject;
import com.basic.common.model.PageResult;
import com.basic.im.entity.ReadDTO;
import com.basic.im.entity.StickDialog;
import com.basic.im.repository.IMongoDAO;
import org.bson.Document;

import java.util.List;
import java.util.Set;

public interface TigaseMsgDao extends IMongoDAO<Object,Integer> {

    void lastMsgDBCreateIndexs();

    void mucMsgDBCreateIndexs();

    Document getLastBody(int sender, int receiver);
    List<Object> getMsgList(int userId,int pageIndex,int pageSize);

    Object getMsgList(int sender,int receiver,int pageIndex,int pageSize);

    void updateMsgIsReadStatus(int userId, String msgId);

    /**
     * 清除最后一条聊天记录
     */
    void deleteLastMsg(String userId,String jid);

    /**
     * 清除某个用户的所有的最后一条聊天记录
     */
    void deleteUserAllLastMsg(String userId);

    long getMsgCountNum();

    List<Object> getChatMsgCount(String startDate, String endDate,int counType);

    /**
     * 清除用户好友的历史消息
     * 及清空最后一条消息记录
     */
    void cleanUserFriendHistoryMsg(int userId,String serverName);

    void destroyUserMsgRecord(int userId);

    void destroyFriendMessage(int userId, int toUserId);

    void deleteRoomMemberMessage(String jid, Integer userId);

    void destroyRoomMessage(String... jids);

    void destroyAllSystemMessage();

    /**
     * 清空好友聊天记录
     * @param userId 自己的用户ID
     * @param toUserId 好友用户Id
     * @param type 1 清除所有好友聊天记录 ,0单方面清除  2 清空好友双方聊天记录
     */
    void cleanFriendMessage(int userId,int toUserId,int type);

    List<Object> getGroupMsgCount(String roomId, String startDate, String endDate, short counType);

    /**
     * 查询条聊天记录用于收藏
     */
    Document queryCollectMessage(int userId,String roomJid, String messageId);


    List<Document> queryLastChatList(int userId ,long startTime, int pageSize);

    /**
     * 查询条聊天记录
     */
    Document queryMessage(int userId,String roomJid, String messageId);

    /**
     * 查询指定群组或单聊的聊天记录
     * @param roomJid 不指定则查询单聊
     */
    List<Document> queryMsgDocument(int userId,String roomJid, List<String> messageIds);

    /**
     * 删除指定时间内的群聊聊天记录
     */
    void deleteGroupMsgBytime(long startTime, long endTime, String room_jid_id);
    /**
     删除过期的 群组聊天消息
     */
    void deleteOutTimeMucMsg();
    /**
     漫游好友聊天记录
     */
    List<Document> queryChatMessageRecord(int userId,int toUserId,long startTime,
                                          long endTime,int pageIndex,
                                          int pageSize, int maxType);

    /**
     漫游群组聊天记录
     */
    List<Document> queryMucMsgs( String roomJid,  long startTime,
                                 long endTime,  int pageIndex,
                                 int pageSize,int maxType,boolean flag);
    /**
     * 查询好友的聊天记录
     * @param sender 自己的用户ID
     * @param receiver 好友的用户ID
     */
    PageResult<Document> queryFirendMsgRecord(Integer sender, Integer receiver, Integer page, Integer limit);


    /**
     * 查询客服相关的消息
     */
    List<Document> queryVisitorMsgRecord(long sender, long receiver, double startTime, int limit);

   /* List<Document> queryServicerLastChatList(long companyMpId,List<String> visitorIdList);*/

    List<Document> queryServicerLastChatList(long companyMpId, long serviceId, long startTime, int pageSize);


    PageResult<Document> queryActiveGroupList(int page, int limit);

    /**
     删除 tigase 超过100条的聊天历史记录
     */
    void deleteMucHistory();

    /**
     * 清除群组历史聊天记录
     * @param roomJid  群组jid
     */
    void dropRoomChatHistory(String roomJid);
    /**
     * 删除tigase 自己的群组历史聊天记录
     * @param roomJid
     */
    void cleanTigaseMuc_History(String roomJid);


    void cleanRoomTigase_Nodes(String roomJid);

    List<Document> queryLastChatList(String userId,long startTime, long endTime, int pageSize, List<String> roomJidList);

    List<Document> queryLastChatList(String userId, long startTime, long endTime, int pageSize, List<String> roomJidList, int needId, List<StickDialog> stickDialogs);

    /**
     * 定时删除  过期的单聊聊天记录
     */
    void deleteTimeOutChatMsgRecord();


    /**
     * 删除消息接口
     * type 1 单聊 2 群聊
     * delete 1 删除属于自己的消息记录 2：撤回 删除 整条消息记录
     **/
    void deleteMsgUpdateLastMessage(int sender, int toUserId, String roomJid, String messageId, int delete, int type);

    /**
     * 更新单聊好友聊天消息状态
     * @param messageId
     */
    void changeMsgReadStatus(String messageId,int userId,int toUserId);

    /**
     * 管理后台查询
     * 查询单聊记录列表
     **/
    PageResult<Document> chat_logs_all(long startTime, long endTime, int sender,
                                       int receiver, int page, int limit, String keyWord,String documents) throws Exception;
    /**
     * 管理后台查询
     * 查询单聊总数
     **/
    long chat_logs_all_count(long startTime, long endTime, int sender,
                                       int receiver, int page, int limit, String keyWord,String documents) throws Exception;

    void chat_logs_all_del(long startTime, long endTime, int sender,
                           int receiver, int pageIndex, int pageSize)throws Exception;

    void deleteChatMsgs(String msgId, int type, String collectionName);
    void changeMsgContent(String messageId, Integer sender, Integer receiver, String content);

    PageResult<Document>  groupchat_logs_all(long startTime, long endTime, String room_jid_id,
                                             int page, int limit, String keyWord, String sender);

    void groupchat_logs_all_del(long startTime, long endTime,
                                String msgId, String room_jid_id)throws Exception;

    void groupchatMsgDel(String roomJid,
                         int type);

    PageResult<Document> roomDetail(int page, int limit, String room_jid_id);

    /**
     * 获取tigase库下的所有文档名称
     **/
    Set<String> findTigaseDocuments();

    /**
     * 获取chat_msgs库下的所有文档名称
     **/
    Set<String> findChatMsgsDocuments();

    List<Document> queryChatMessageBySeqNo(long userId, long toUserId, long startSeqNo, long endSeqNo, int pageIndex, int pageSize);
    List<Document> queryGroupMessageBySeqNo(String roomJid, long startSeqNo, long endSeqNo, int pageIndex, int pageSize);
    List<Document> pullMessageBySeqNos(long userId, long toUserId, Set<Long> seqNos);

    List<Document> pullGroupMessageBySeqNos(String roomJid, Set<Long> seqNoSets);

    List<Document> queryMultipointChat(Integer userId, long startTime, long endTime, String from, String to, int pageSize);

    List<? extends Object> queryRoomMessageReadList(int userId, String roomJid, String messageId, int isRead, int pageIndex, int pageSize);

    long queryRoomMessageReadCount(Integer verifyUserId, String roomJid, String messageId);

    List<ReadDTO> queryRoomMessageReadLastTime(Long lastTime, Integer userId);


    JSONObject queryChatDetails(Integer userId, List<Integer> userIds, List<String> roomIds);

    List<Document> queryStickDialogChatList(List<StickDialog> stickDialogs, int needId);

    /**
     * 根据类型 删除群组消息
     * @param content 内容
     * @param room_jid_id 群jid
     * @param type 类型
     * @throws Exception
     */
    void delete_type_message_room(Object content, String room_jid_id, int type);

    PageResult<Document> getMsgRevokeRecordList (long startTime, long endTime, int sender,int receiver, int page, int limit, String keyWord);


    List<Document> querySingleChat(int userId,int toUserId, int pageIndex,  int pageSize, long startTime, long endTime, int type, String content);

    List<Document> queryGroupChat(String roomJid,int pageIndex, int pageSize, long startTime, long endTime, int type, String content, boolean flag, int memberId);

    void groupchat_logs_all_edit(String messageId, String room_jid_id, String content);

    void editMsgUpdateLastMessage(int sender, int toUserId, String roomJid, String messageId, int type, String message);
}
