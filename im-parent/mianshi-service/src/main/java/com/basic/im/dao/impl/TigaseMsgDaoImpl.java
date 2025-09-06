package com.basic.im.dao.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.basic.common.model.PageResult;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.constants.DBConstants;
import com.basic.im.entity.ReadDTO;
import com.basic.im.entity.ReadUserDTO;
import com.basic.im.entity.StickDialog;
import com.basic.im.message.MessageType;
import com.basic.im.message.dao.TigaseMsgDao;
import com.basic.im.repository.MongoOperator;
import com.basic.im.repository.MongoRepository;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.entity.AuthKeys;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.user.service.UserManager;
import com.basic.im.user.service.impl.AuthKeysServiceImpl;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.mongodb.springdata.MongoConfig;
import com.basic.utils.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import javax.print.Doc;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author zhm
 * @version V1.0
 * @Description:
 * @date 2019/9/3 19:27
 */
@Repository
public class TigaseMsgDaoImpl extends MongoRepository<Object, Integer> implements TigaseMsgDao, InitializingBean {

    private Logger lastChatLog = LoggerFactory.getLogger("lastChatLog");


    @Autowired(required = false)
    @Qualifier(value = "imRoomMongoClient")
    private MongoClient imRoomMongoClient;

    @Autowired(required = false)
    private MongoConfig mongoConfig;

    @Autowired(required = false)
    @Qualifier(value = "mongoTemplateForChatMsgs")
    private MongoTemplate mongoTemplateForChatMsgs;

    private MongoDatabase chatMsgDB;

    public MongoDatabase getChatMsgDB() {
        return chatMsgDB;
    }

    private MongoDatabase offlineDB;

    public MongoDatabase getOfflineDB() {
        return offlineDB;
    }

    @Override
    public MongoTemplate getDatastore() {
        return SKBeanUtils.getDatastore();
    }

    @Override
    public Class<Object> getEntityClass() {
        return Object.class;
    }

    private MongoDatabase lastMsgDB;

    public MongoDatabase getLastMsgDB() {
        return lastMsgDB;
    }

    private MongoDatabase imRoomDB;

    public MongoDatabase getImRoomDB() {
        return imRoomDB;
    }

    @Autowired(required = false)
    private UserCoreService userCoreService;

    private static final String CHATMSG_DBNAME = "chat_msgs";

    public final String MUCMSG = "mucmsg_";

    protected static final int DB_REMAINDER = 10000;

    @Autowired
    private RoomManagerImplForIM roomManagerImpl;

    @Autowired
    private UserManager userManager;

    /**
     * 群组已读消息表
     */
    private final static String ROOM_MSG_RED = "room_msg_read";

    /**
     * 返回值包装
     */
    private final Function<List<Document>, List<Document>> chatListFunction = (resultList -> {
        resultList.forEach(obj -> {
            String targetId = obj.getString("jid");
            if (KConstants.ONE == obj.getInteger("isRoom")) {
                targetId = roomManagerImpl.getRoomIdByJid(targetId);
            }
            obj.put("targetId", targetId);
        });
        return resultList;
    });

    @Override
    public String getCollectionName(int userId) {
        int remainder = 0;
        if (userId > KConstants.MIN_USERID) {
            remainder = userId / DB_REMAINDER;
        }
        return String.valueOf(remainder);
    }

    @Override
    public MongoCollection<Document> getMongoCollection(MongoDatabase database, int userId) {
        int remainder = 0;
        if (userId > KConstants.MIN_USERID) {
            remainder = userId / DB_REMAINDER;
        }
        return database.getCollection(String.valueOf(remainder));
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        if (null != imRoomMongoClient) {
            chatMsgDB = imRoomMongoClient.getDatabase(DBConstants.CHAT_MSGS_DB);
            lastMsgDB = imRoomMongoClient.getDatabase(DBConstants.LASTCHAT_DB);
            offlineDB = imRoomMongoClient.getDatabase(DBConstants.OFFLINE_CHAT_DB);
            String roomDbName = mongoConfig.getRoomDbName();
            imRoomDB = imRoomMongoClient.getDatabase(roomDbName);
        }

        createDbIndexs();

    }

    public void createDbIndexs() {
        ThreadUtils.executeInThread(obj -> {
            chatMsgDBCreateIndexs();
            offlineDBCreateIndexs();
            lastMsgDBCreateIndexs();
            mucMsgDBCreateIndexs();
            roomReadMsgCreateIndexs();

        });
    }

    public void chatMsgDBCreateIndexs() {

        chatMsgDB.listCollectionNames().iterator().forEachRemaining(name -> {
            MongoCollection<Document> collection = chatMsgDB.getCollection(name);
            collection.createIndex(new Document("messageId", 1));
            collection.createIndex(new Document("seqNo", 1));

            collection.createIndex(
                    new Document("sender", 1).append("receiver", 1)
                            .append("seqNo", 1).append("timeSend", -1)
            );

            collection.createIndex(
                    new Document("sender", 1).append("receiver", 1)
                            .append("direction", 0)
            );
        });
    }

    public void offlineDBCreateIndexs() {
        offlineDB.listCollectionNames().iterator().forEachRemaining(name -> {
            MongoCollection<Document> collection = offlineDB.getCollection(name);
            collection.createIndex(new Document("to", 1));
            collection.createIndex(new Document("receiver", 1).append("timeSend", 1));
        });
    }

    @Override
    public void lastMsgDBCreateIndexs() {

        lastMsgDB.listCollectionNames().iterator().forEachRemaining(name -> {
            if (!DBConstants.LASTCHAT_MUC_COLLECTION.equals(name)) {
                MongoCollection<Document> collection = lastMsgDB.getCollection(name);
                collection.createIndex(new Document("messageId", 1));
                collection.createIndex(
                        new Document("timeSend", 1).append("userId", 1).append("isRoom", 1).append("jid", 1)
                );

            } else {
                MongoCollection<Document> collection = lastMsgDB.getCollection(name);
                collection.createIndex(new Document("messageId", 1));

                collection.createIndex(new Document("userId", 1).append("jid", 1));
                collection.createIndex(
                        new Document("timeSend", 1).append("userId", 1)
                                .append("isRoom", 1).append("jid", 1)
                );

            }

        });
    }

    @Override
    public void mucMsgDBCreateIndexs() {

        imRoomDB.listCollectionNames().iterator().forEachRemaining(name -> {
            if (name.startsWith(MUCMSG)) {
                MongoCollection<Document> collection = imRoomDB.getCollection(name);
                if (collection.countDocuments() < 2000) {
                    collection.createIndex(new Document("seqNo", 1));
                    collection.createIndex(new Document("messageId", 1));
                    collection.createIndex(
                            new Document("room_jid", 1).append("seqNo", 1).append("timeSend", 1)
                    );
                }

            }
        });
    }

    public void removeMucMsgDBCreateIndexs() {

        imRoomDB.listCollectionNames().iterator().forEachRemaining(name -> {
            if (name.startsWith(MUCMSG)) {
                MongoCollection<Document> collection = imRoomDB.getCollection(name);
                if (collection.countDocuments() < 5000) {
                    collection.dropIndexes();
                }
            }
        });
    }

    public void roomReadMsgCreateIndexs() {
        MongoCollection<Document> collection = mongoTemplate.getCollection(ROOM_MSG_RED);
        collection
                .createIndex(new Document("userId", 1).append("modifyTime", 1));
        collection
                .createIndex(new Document("roomJid", 1).append("messageId", 1));

    }

    @Override
    public Document getLastBody(int sender, int receiver) {
        MongoCollection<Document> dbCollection = getMsgRepostory(sender);
        Document q = new Document();
        q.put("sender", sender + "");
        q.put("receiver", receiver + "");
        Document dbObj = dbCollection.find(q).sort(new Document("ts", -1)).first();
        if (null == dbObj) {
            return null;
        }
        return dbObj;
    }

    public MongoCollection<Document> getMsgRepostory(int sender) {
        return chatMsgDB.getCollection(getCollectionName(sender));
    }

    @Override
    public List<Object> getMsgList(int userId, int pageIndex, int pageSize) {
        List<Object> msgList = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getMsgRepostory(userId);
        // 分组条件
        Document groupFileds = new Document();
        groupFileds.put("sender", "$sender");
        // 过滤条件
        Document map = new Document();
        map.put("receiver", userId + "");
        map.put("direction", 0);
        map.put("isRead", 0);
        Document macth = new Document("$match", new Document(map));

        Document fileds = new Document("_id", groupFileds);
        fileds.put("count", new BasicDBObject("$sum", 1));
        Document group = new Document("$group", fileds);
        Document limit = new Document("$limit", pageSize);
        Document skip = new Document("$skip", pageIndex * pageSize);
        AggregateIterable<Document> out = dbCollection.aggregate(Arrays.asList(macth, group, skip, limit));
        MongoCursor<Document> iterator = out.iterator();

        try {
            while (iterator.hasNext()) {
                Document document = iterator.next();
                Document dbObj = (Document) document.get("_id");
                dbObj.append("count", document.get("count"));
                int sender = dbObj.getInteger("sender");
                int receiver = userId;
                String nickname = userCoreService.getNickName(sender);
                if (StringUtil.isEmpty(nickname)) {
                    continue;
                }

                int count = dbObj.getInteger("count");

                dbObj.put("nickname", nickname);
                dbObj.put("count", count);
                dbObj.put("sender", sender + "");
                dbObj.put("receiver", receiver + "");
                Document lastBody = getLastBody(sender, receiver);
                dbObj.put("body", lastBody.get("content"));
                JSONObject body = JSONObject.parseObject(lastBody.getString("message"));
                if (null != body.get("isEncrypt") && true == body.getBoolean("isEncrypt")) {
                    dbObj.put("isEncrypt", 1);
                } else {
                    dbObj.put("isEncrypt", 0);
                }
                dbObj.put("messageId", lastBody.get("messageId"));
                dbObj.put("timeSend", lastBody.get("timeSend"));
                msgList.add(dbObj);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            iterator.close();
        }
        return msgList;
    }

    @Override
    public Object getMsgList(int sender, int receiver, int pageIndex, int pageSize) {
        List<Document> msgList = Lists.newArrayList();
        Document q = new Document();
        q.put("sender", sender + "");
        q.put("receiver", receiver + "");
        q.put("direction", 0);
        q.put("isRead", 0);
        MongoCollection<Document> dbCollection = getMsgRepostory(sender);
        MongoCursor<Document> cursor = dbCollection.find(q).iterator();
        while (cursor.hasNext()) {
            Document dbObj = cursor.next();
            dbObj.put("nickname", userCoreService.getNickName(sender));
            dbObj.put("content", dbObj.get("content").toString());
            // 处理body
            JSONObject body = JSONObject.parseObject(dbObj.getString("message"));
            if (null != body.get("isEncrypt") && "1".equals(body.get("isEncrypt").toString())) {
                dbObj.put("isEncrypt", 1);
            } else {
                dbObj.put("isEncrypt", 0);
            }
            dbObj.put("timeSend", dbObj.get("timeSend"));
            //System.out.println("dbobj : "+JSONObject.toJSONString(dbObj));
            msgList.add(dbObj);
        }
        return msgList;
    }

    @Override
    public void updateMsgIsReadStatus(int userId, String msgId) {
        Document query = new Document("messageId", msgId);
        getMsgRepostory(userId).
                updateOne(query, new Document("$set", new Document("isRead", 1)));
    }

    @Override
    public void deleteLastMsg(String userId, String jid) {
        MongoCollection<Document> collection = getMongoCollection(lastMsgDB, Integer.valueOf(userId));
        Document query = new Document("jid", jid);
        if (!StringUtil.isEmpty(userId)) {
            query.append("userId", userId);
        }
        collection.deleteMany(query);
        lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(query) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
    }


    @Override
    public void deleteUserAllLastMsg(String userId) {
        MongoCollection<Document> collection = null;
        Document query = new Document();
        List<Document> orQuery = new ArrayList<>();
        orQuery.add(new Document("userId", userId));
        orQuery.add(new Document("jid", userId));
        query.append(MongoOperator.OR, orQuery);

        MongoIterable<String> listCollectionNames = getLastMsgDB().listCollectionNames();
        MongoCollection<Document> dbCollection = null;
        for (String collectionName : listCollectionNames) {
            dbCollection = getLastMsgDB().getCollection(collectionName);
            dbCollection.deleteMany(query);
            //deleteByQuery(query,collectionName);
        }
        lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(query) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
    }

    /**
     * 获取单聊消息数量
     */
    @Override
    public long getMsgCountNum() {
        long count = 0;
        MongoIterable<String> listCollectionNames = getChatMsgDB().listCollectionNames();
        for (String string : listCollectionNames) {
            count += getChatMsgDB().getCollection(string).count(new Document("direction", 0));
        }
        return count;
    }

    /**
     * 单聊消息数量统计      时间单位  每日、每月、每分钟、每小时
     *
     * @param counType 统计类型   1: 每个月的数据      2:每天的数据       3.每小时数据   4.每分钟的数据 (小时)
     */
    @Override
    public List<Object> getChatMsgCount(String startDate, String endDate, int counType) {
        List<Object> countData;
        long startTime = 0; //开始时间（秒）
        long endTime = 0; //结束时间（秒）,默认为当前时间
        /**
         * 如时间单位为月和天，默认开始时间为当前时间的一年前 ; 时间单位为小时，默认开始时间为当前时间的一个月前;
         * 时间单位为分钟，则默认开始时间为当前这一天的0点
         */
        long defStartTime = counType == 4 ? com.basic.utils.DateUtil.getTodayMorning().getTime() / 1000
                : counType == 3 ? com.basic.utils.DateUtil.getLastMonth().getTime() / 1000 : com.basic.im.comm.utils.DateUtil.getLastYear().getTime() / 1000;


        startTime = StringUtil.isEmpty(startDate) ? defStartTime : DateUtil.toDate(startDate).getTime();
        endTime = StringUtil.isEmpty(endDate) ? System.currentTimeMillis() : DateUtil.toDate(endDate).getTime();

        Document queryTime = new Document("$ne", null);

        if (startTime != 0 && endTime != 0) {
            queryTime.append("$gt", startTime);
            queryTime.append("$lt", endTime);
        }

        Document query = new Document("ts", queryTime);


        String mapStr = "function Map() { "
                + "var date = new Date(this.ts);"
                + "var year = date.getFullYear();"
                + "var month = (\"0\" + (date.getMonth()+1)).slice(-2);"  //month 从0开始，此处要加1
                + "var day = (\"0\" + date.getDate()).slice(-2);"
                + "var hour = (\"0\" + date.getHours()).slice(-2);"
                + "var minute = (\"0\" + date.getMinutes()).slice(-2);"
                + "var dateStr = date.getFullYear()" + "+'-'+" + "(parseInt(date.getMonth())+1)" + "+'-'+" + "date.getDate();";

        if (counType == 1) { // counType=1: 每个月的数据
            mapStr += "var key= year + '-'+ month;";
        } else if (counType == 2) { // counType=2:每天的数据
            mapStr += "var key= year + '-'+ month + '-' + day;";
        } else if (counType == 3) { //counType=3 :每小时数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour +' : 00';";
        } else if (counType == 4) { //counType=4 :每分钟的数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour + ':'+ minute;";
        }

        mapStr += "emit(key,1);}";

        String reduce = "function Reduce(key, values) {" +
                "return Array.sum(values);" +
                "}";

        //Map<String,Double> map = new HashMap<String,Double>();
        Map<String, Double> map = new TreeMap<>();
        //获得单聊消息集合对象
        MongoIterable<String> collectionNames = getChatMsgDB().listCollectionNames();
        MongoCollection<Document> collection = null;
        MongoCursor<Document> iterator = null;
        Document obj = null;
        for (String str : collectionNames) {
            collection = getChatMsgDB().getCollection(str, Document.class);
            if (0 == collection.count(query))
                continue;
            iterator = collection.mapReduce(mapStr, reduce, Document.class).filter(query).iterator();
            Double value;
            String id;
            while (iterator.hasNext()) {
                obj = iterator.next();
                id = (String) obj.get("_id");
                value = (Double) obj.get("value");
                if (null != map.get(id)) {
                    map.put(id, map.get(id) + value);
                } else {
                    map.put(id, value);
                }
                //System.out.println("====>>>>单聊消息"+JSON.toJSON(obj));
            }
            if (null != iterator) {
                iterator.close();
            }
        }

        countData = map.entrySet().stream().collect(Collectors.toList());
        map.clear();

        return countData;
    }

    /**
     * 群聊消息数量统计      时间单位  每日、每月、每分钟、每小时
     *
     * @param startDate
     * @param endDate
     * @param counType  统计类型   1: 每个月的数据      2:每天的数据       3.每小时数据   4.每分钟的数据 (小时)
     */
    @Override
    public List<Object> getGroupMsgCount(String roomId, String startDate, String endDate, short counType) {

        //获得群聊消息集合对象
        MongoCollection<Document> collection = getImRoomDB().getCollection(MUCMSG + roomId);

        if (collection == null || collection.countDocuments() == 0) {
            System.out.println("暂无数据");
            throw new ServiceException("暂无数据");
        }

        List<Object> countData = new ArrayList<Object>();

        long startTime = 0; //开始时间（秒）

        long endTime = 0; //结束时间（秒）,默认为当前时间

        /**
         * 如时间单位为月和天，默认开始时间为当前时间的一年前 ; 时间单位为小时，默认开始时间为当前时间的一个月前;
         * 时间单位为分钟，则默认开始时间为当前这一天的0点
         */
        long defStartTime = counType == 4 ? DateUtil.getTodayMorning().getTime()
                : counType == 3 ? DateUtil.getLastMonth().getTime() : DateUtil.getLastYear().getTime();

        startTime = StringUtil.isEmpty(startDate) ? defStartTime : DateUtil.toDate(startDate).getTime();
        endTime = StringUtil.isEmpty(endDate) ? System.currentTimeMillis() : DateUtil.toDate(endDate).getTime();

        Document queryTime = new Document("$ne", null);

        if (startTime != 0 && endTime != 0) {
            queryTime.append("$gt", startTime);
            queryTime.append("$lt", endTime);
        }

        Document query = new Document("ts", queryTime);


        String mapStr = "function Map() { "
                + "var date = new Date(this.ts);"
                + "var year = date.getFullYear();"
                + "var month = (\"0\" + (date.getMonth()+1)).slice(-2);"  //month 从0开始，此处要加1
                + "var day = (\"0\" + date.getDate()).slice(-2);"
                + "var hour = (\"0\" + date.getHours()).slice(-2);"
                + "var minute = (\"0\" + date.getMinutes()).slice(-2);"
                + "var dateStr = date.getFullYear()" + "+'-'+" + "(parseInt(date.getMonth())+1)" + "+'-'+" + "date.getDate();";

        if (counType == 1) { // counType=1: 每个月的数据
            mapStr += "var key= year + '-'+ month;";
        } else if (counType == 2) { // counType=2:每天的数据
            mapStr += "var key= year + '-'+ month + '-' + day;";
        } else if (counType == 3) { //counType=3 :每小时数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour +' : 00';";
        } else if (counType == 4) { //counType=4 :每分钟的数据
            mapStr += "var key= year + '-'+ month + '-' + day + '  ' + hour + ':'+ minute;";
        }

        mapStr += "emit(key,1);}";

        String reduce = "function Reduce(key, values) {" +
                "return Array.sum(values);" +
                "}";
        MapReduceIterable<Document> mapReduceIterable = collection.mapReduce(mapStr, reduce);
        mapReduceIterable.filter(query);
        MongoCursor<Document> iterator = mapReduceIterable.iterator();

        Map<String, Double> map = new HashMap<String, Double>();
        try {
            while (iterator.hasNext()) {
                Document obj = iterator.next();

                map.put((String) obj.get("_id"), (Double) obj.get("value"));
                countData.add(JSON.toJSON(map));
                map.clear();
                //System.out.println("======>>>群消息统计 "+JSON.toJSON(obj));

            }
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }


        return countData;
    }

    @Override
    public void cleanUserFriendHistoryMsg(int userId, String serverName) {
        if (userId == 0) {
            return;
        }
        ThreadUtils.executeInThread(obj -> {
            MongoCollection<Document> collection = getCollection(lastMsgDB, userId);
            Document query = new Document("userId", userId + "");
            collection.deleteMany(query);
            MongoIterable<String> listCollectionNames = lastMsgDB.listCollectionNames();
            for (String dbname : listCollectionNames) {
                collection = lastMsgDB.getCollection(dbname);
                query = new Document("jid", userId + "");
                collection.deleteMany(query);
            }
            //lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(query) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());

            listCollectionNames = chatMsgDB.listCollectionNames();

            MongoCollection<Document> msgRepostory = getMsgRepostory(userId);
            query = new Document("sender", userId + "");
            msgRepostory.deleteMany(query);
            for (String dbname : listCollectionNames) {
                msgRepostory = chatMsgDB.getCollection(dbname);
                query = new Document("receiver", userId + "");
                msgRepostory.deleteMany(query);
            }


            // 删除对应的离线消息(不必serverName)
            MongoCollection<Document> msgHistory = getOfflineDB().getCollection(getCollectionName(userId));
            query = new Document("to", userId + "");
            msgHistory.deleteMany(query);
            listCollectionNames = offlineDB.listCollectionNames();

            for (String dbname : listCollectionNames) {
                msgRepostory = offlineDB.getCollection(dbname);
                query = new Document("from", userId + "");
                msgRepostory.deleteMany(query);
            }
        });


    }

    @Override
    public void destroyUserMsgRecord(int userId) {
        ThreadUtils.executeInThread(obj -> {
            DBCursor cursor = null;
            MongoCursor<String> mongoCursor = null;
            MongoCollection<Document> dbCollection = getMsgRepostory(userId);
            MongoCollection<Document> lastdbCollection;
            try {

                lastdbCollection = getCollection(lastMsgDB, userId);
                Document query = new Document();
                Document lastquery = new Document();
                query.append("sender", userId + "");
                query.append("deleteTime", new Document(MongoOperator.GT, 0)
                                .append(MongoOperator.LT, DateUtil.currentTimeSeconds()))
                        .append("isRead", 1);

                Document base = dbCollection.find(query).first();
                List<Document> queryOr = new ArrayList<>();
                if (base != null) {
                    queryOr.add(new Document("jid", String.valueOf(base.get("sender"))).append("userId", base.get("receiver").toString()));
                    queryOr.add(new Document("userId", String.valueOf(base.get("sender"))).append("jid", base.get("receiver").toString()));
                    lastquery.append(MongoOperator.OR, queryOr);
                } else {
                    return;
                }
                // 删除文件
                query.append("contentType", new Document(MongoOperator.IN, MsgType.FileTypeArr));
                mongoCursor = dbCollection.distinct("content", query, String.class).iterator();

                while (mongoCursor.hasNext()) {
                    try {
                        ConstantUtil.deleteFile(mongoCursor.next());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

                query.remove("contentType");

                dbCollection.deleteMany(query); //将消息记录中的数据删除

                // 重新查询一条消息记录插入
                List<Document> baslist = new ArrayList<>();
                if (base != null) {
                    baslist.add(new Document("receiver", base.get("sender")));
                    baslist.add(new Document("sender", base.get("sender")));
                    query.append(MongoOperator.OR, baslist);
                }

                query.remove("sender");
                query.remove("deleteTime");
                query.remove("isRead");
                Document lastMsgObj = dbCollection.find(query).sort(new Document("timeSend", -1)).first();

                if (lastMsgObj != null) {
                    Document values = new Document();
                    values.put("messageId", lastMsgObj.get("messageId"));
                    values.put("timeSend", new Double(lastMsgObj.get("timeSend").toString()).longValue());
                    values.put("content", lastMsgObj.get("content"));
                    if (!lastquery.isEmpty()) {
                        lastdbCollection.updateMany(lastquery, new Document(MongoOperator.SET, values));
                        lastChatLog.info("改变最后一条消息记录 =====> query " + JSONObject.toJSONString(lastquery) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                    }

                } else {
                    return;
                }

                if (!lastquery.isEmpty()) {
                    lastdbCollection.deleteMany(lastquery);
                    lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(query) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
                if (null != mongoCursor) {
                    mongoCursor.close();
                }
            }
        });
    }

    @Override
    public void destroyFriendMessage(int userId, int toUserId) {
        // 群聊、单聊消息
        // 最后一条聊天消息
        MongoCollection<Document> dbCollection = getMongoCollection(getChatMsgDB(), userId);

        MongoCollection<Document> lastdbCollection = getMongoCollection(getLastMsgDB(), userId);

        MongoCollection<Document> historyCollection = getMongoCollection(getOfflineDB(), userId);

        List<Document> queryOr = new ArrayList<>();

        /**
         * 清除历史消息
         */

        queryOr.add(new Document("sender", userId + "").append("receiver", toUserId + ""));
        queryOr.add(new Document("sender", toUserId + "").append("receiver", userId + ""));

        historyCollection.deleteMany(new Document("$or", queryOr));
        historyCollection = getMongoCollection(getOfflineDB(), toUserId);
        historyCollection.deleteMany(new Document("$or", queryOr));

        /**
         * 删除双方
         */
        queryOr = new ArrayList<>();
        queryOr.add(new Document("sender", userId + "").append("receiver", toUserId + ""));
        queryOr.add(new Document("sender", toUserId + "").append("receiver", userId + ""));

        dbCollection.deleteMany(new Document("$or", queryOr));
        dbCollection = getMongoCollection(getChatMsgDB(), toUserId);
        dbCollection.deleteMany(new Document("$or", queryOr));

        BasicDBObject lastqueryAll = new BasicDBObject("isRoom", 0);
        queryOr = new ArrayList<>();

        queryOr.add(new Document("userId", userId).append("jid", toUserId));
        queryOr.add(new Document("userId", toUserId).append("jid", userId));

        lastdbCollection.deleteMany(lastqueryAll);
        lastdbCollection = getMongoCollection(getLastMsgDB(), toUserId);
        lastdbCollection.deleteMany(lastqueryAll);
    }

    @Override
    public void deleteRoomMemberMessage(String jid, Integer userId) {
        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + jid);


        dbCollection.deleteMany(new Document("sender", userId));

    }

    @Override
    public void destroyRoomMessage(String... jids) {
        MongoCollection<Document> dbCollection;
        for (String jid : jids) {
            dbCollection = getImRoomDB().getCollection(MUCMSG + jid);
            dbCollection.drop();
        }
        MongoCollection<Document> lastdbCollection = getLastMsgDB().getCollection(DBConstants.LASTCHAT_MUC_COLLECTION);

        lastdbCollection.deleteMany(new Document("jid", new Document(QueryOperators.IN, Arrays.asList(jids))));


    }

    @Override
    public void destroyAllSystemMessage() {
        getChatMsgDB().drop();
        getLastMsgDB().drop();
        for (String collectionName : getImRoomDB().listCollectionNames()) {
            if (collectionName.startsWith(MUCMSG)) {
                getImRoomDB().getCollection(collectionName).drop();
            }
        }
    }


    /**
     * @param userId   自己的用户ID
     * @param toUserId 好友用户Id
     * @param type     1 清除所有好友聊天记录 ,0单方面清除  2 清空好友双方聊天记录
     */
    @Override
    public void cleanFriendMessage(int userId, int toUserId, int type) {
        // 群聊、单聊消息
        MongoCollection<Document> dbCollection = null;
        // 最后一条聊天消息
        MongoCollection<Document> lastdbCollection = null;
        List<Document> queryOr = new ArrayList<>();
        try {

            dbCollection = getMsgRepostory(userId);
            lastdbCollection = getMongoCollection(lastMsgDB, userId);

            BasicDBObject queryAll = new BasicDBObject();

            BasicDBObject lastqueryAll = new BasicDBObject();
            if (type == 1) {
                queryAll.append("sender", userId + "");
                //queryAll.append("receiver", toUserId + "");
                /*
                 * queryAll.append("contentType", new BasicDBObject(MongoOperator.IN,
                 * MsgType.FileTypeArr)); List<String> fileList=dbCollection.distinct("content",
                 * queryAll); for(String fileUrl:fileList){ // 调用删除方法将文件从服务器删除
                 * ConstantUtil.deleteFile(fileUrl); } queryAll.remove("contentType");
                 */

                Document baseAll = dbCollection.find(queryAll).first();

                if (null != baseAll) {
                        /*queryOr.add(new Document("userId", String.valueOf(baseAll.get("sender"))).append("jid",
                            baseAll.get("receiver").toString()));*/
                    queryOr.add(new Document("userId", userId + "").append("jid", toUserId + ""));
                    queryOr.add(new Document("jid", toUserId + "").append("userId", userId + ""));


                    lastqueryAll.append(MongoOperator.OR, queryOr);
                }
                if (!lastqueryAll.isEmpty()) {
                    lastdbCollection.deleteMany(lastqueryAll);
                }
                lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(lastqueryAll) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                dbCollection.deleteMany(queryAll);
                return;
            } else {
                if (0 == type) {
                    BasicDBObject query = new BasicDBObject();

                    query.append("sender", userId + "");
                    query.append("receiver", toUserId + "");


                    /*
                     * query.append("contentType", new BasicDBObject(MongoOperator.IN,
                     * MsgType.FileTypeArr)); List<String> fileList=dbCollection.distinct("content",
                     * query); for(String fileUrl:fileList){ // 调用删除方法将文件从服务器删除
                     * ConstantUtil.deleteFile(fileUrl); } query.remove("contentType");
                     */


                    int delayedTime = SKBeanUtils.getSystemConfig().getMsgDelayedDeleteTime();

                    if (0 < delayedTime) {
                        // 添加消息标志位，拉取漫游过滤
                        long currentTime = DateUtil.currentTimeSeconds();
                        Document withdrawVal = new Document();
                        withdrawVal.put("delayedTime", currentTime + delayedTime);
                        withdrawVal.put("deleteTime", currentTime + delayedTime);
                        dbCollection.updateMany(query, new Document(MongoOperator.SET, withdrawVal));

                        Document lastquery = new Document("userId", userId + "").append("jid", toUserId + "");
                        if (!lastquery.isEmpty()) {
                            lastdbCollection.deleteMany(lastquery);
                        }
                        lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(lastquery) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                    } else {
                        // 删除单聊离线消息
                        //delOfficeMsg(messageId);
                        dbCollection.deleteMany(query);

                        Document lastquery = new Document("userId", userId + "").append("jid", toUserId + "");
                        if (!lastquery.isEmpty()) {
                            lastdbCollection.deleteMany(lastquery);
                        }
                        lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(lastquery) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                    }
                    // 删除消息记录
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Document queryCollectMessage(int userId, String roomJid, String messageId) {
        MongoCollection<Document> dbCollection = null;
        Document data = null;
        if (StringUtil.isEmpty(roomJid)) {
            dbCollection = getMsgRepostory(userId);
        } else {
            dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
        }
        Document query = new Document();
        query.put("messageId", messageId);
        // Document  project= new Document("message",0);
        data = dbCollection.find(query).first();
        //log.info(" emojiMsg  文件："+JSONObject.toJSONString(data));
        return data;
    }

    @Override
    public Document queryMessage(int userId, String roomJid, String messageId) {
        MongoCollection<Document> dbCollection = null;
        Document data = null;
        if (StringUtil.isEmpty(roomJid)) {
            dbCollection = getMsgRepostory(userId);
        } else {
            dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
        }
        Document query = new Document();
        query.put("messageId", messageId);
        // Document  project= new Document("message",0);
        data = dbCollection.find(query).first();
        //log.info(" emojiMsg  文件："+JSONObject.toJSONString(data));
        return data;
    }

    @Override
    public List<Document> queryMsgDocument(int userId, String roomJid, List<String> messageIds) {
        List<Document> result = new ArrayList<>();
        MongoCollection<Document> dbCollection = null;
        if (("0").equals(roomJid)) {
            dbCollection = getMsgRepostory(userId);
        } else {
            dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
        }
        Document q = new Document();
        q.put("messageId", new BasicDBObject(MongoOperator.IN, messageIds));
        q.put("sender", userId + "");
        // Document  project= new Document("message",0);
        MongoCursor<Document> dbCursor = dbCollection.find(q).iterator();
//				CourseMessage courseMessage=new CourseMessage();
        while (dbCursor.hasNext()) {
            result.add(dbCursor.next());
//					courseMessage.setCourseMessageId(new ObjectId());
//					courseMessage.setUserId(course.getUserId());
//					courseMessage.setCourseId(course.getCourseId().toString());
//					courseMessage.setCreateTime(String.valueOf(dbObj.get("timeSend")));
//					courseMessage.setMessage(String.valueOf(dbObj));
//					courseMessage.setMessageId(String.valueOf(dbObj.get("messageId")));
//					getUserDao().saveEntity(courseMessage);

        }
        dbCursor.close();
        return result;
    }


    /**
     * 管理后台删除群组聊天记录
     *
     * @param startTime
     * @param endTime
     * @param room_jid
     */
    @Override
    public void deleteGroupMsgBytime(long startTime, long endTime, String room_jid) {
        Document fileQuery = new Document("contentType", new Document(MongoOperator.IN, MsgType.FileTypeArr));
        Document query = new Document();
        if (0 != startTime) {
            query.put("ts", new Document("$gte", startTime));
        }
        if (0 != endTime) {
            query.put("ts", new Document("$gte", endTime));
        }
        MongoCollection<Document> dbCollection = null;
        if (room_jid != null) {
            dbCollection = getImRoomDB().getCollection(MUCMSG + room_jid);
            MongoCursor<Document> iterator = dbCollection.find(fileQuery).projection(new Document("content", 1)).iterator();
            try {
                while (iterator.hasNext()) {

                    Document dbObj = iterator.next();
                    // 解析消息体

                    try {
                        // 调用删除方法将文件从服务器删除
                        ConstantUtil.deleteFile(dbObj.getString("content"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } finally {
                if (null != iterator) {
                    iterator.close();
                }
            }


            // 将消息记录中的数据删除
            dbCollection.deleteMany(query);
        } else {
            MongoCursor<String> jidList = getImRoomDB().getCollection("chat_room").distinct("jid", String.class).iterator();
            MongoCursor<Document> iterator = null;
            while (jidList.hasNext()) {
                dbCollection = getImRoomDB().getCollection(MUCMSG + jidList.next());
                iterator = dbCollection.find(fileQuery).projection(new Document("content", 1)).iterator();
                while (iterator.hasNext()) {

                    Document dbObj = iterator.next();
                    // 解析消息体

                    try {
                        // 调用删除方法将文件从服务器删除
                        ConstantUtil.deleteFile(dbObj.getString("content"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (null != iterator) {
                    iterator.close();
                }
            }
        }


    }


    @Override
    public void deleteOutTimeMucMsg() {
        MongoCollection<Document> dbCollection = null;
        // 最后一条聊天消息
        MongoCollection<Document> lastdbCollection = null;

        Document query = null;
        Document lastquery = null;
        MongoCursor<String> set = null;
        try {
            logger.info("=====> deleteMucMsgRecord " + DateUtil.TimeToStr(new Date()));
            set = getImRoomDB().listCollectionNames().iterator();
            String s = null;
            while (set.hasNext()) {
                s = set.next();
                if (s.startsWith("mucmsg_")) {
                    lastquery = new Document();
                    query = new Document();
                    query.append("deleteTime", new BasicDBObject(MongoOperator.GT, 0)
                            .append(MongoOperator.LT, DateUtil.currentTimeSeconds()));
                    dbCollection = getImRoomDB().getCollection(s);
                    lastdbCollection = getDatastore().getCollection("chat_lastChats");

                    Document base = dbCollection.find(query).first();
                    if (base != null) {
                        lastquery.put("jid", base.get("room_jid"));
                    }
                    /*
                     * if(base!=null) query.put("room_jid", base.get("room_jid"));
                     */

                    // 删除文件
                    query.append("contentType", new Document(MongoOperator.IN, MsgType.FileTypeArr));
                    MongoCursor<String> fileList = dbCollection.distinct("content", query, String.class).iterator();

                    while (fileList.hasNext()) {
                        ConstantUtil.deleteFile(fileList.next());
                    }
                    fileList.close();

                    // 将消息记录中的数据删除
                    query.remove("contentType");
                    dbCollection.deleteMany(query);

                    query.remove("deleteTime");
                    Document lastMsgObj = dbCollection.find(query).sort(new Document("timeSend", -1)).limit(1).first();
                    Document values = new Document();
                    if (lastMsgObj != null) {
                        values.put("messageId", lastMsgObj.get("messageId"));
                        values.put("timeSend", new Double(lastMsgObj.get("timeSend").toString()).longValue());
                        values.put("content", lastMsgObj.get("content"));
                        if (!lastquery.isEmpty()) {
                            lastdbCollection.updateMany(lastquery, new Document(MongoOperator.SET, values));
                            lastChatLog.info("改变最后一条消息记录 =====> query " + JSONObject.toJSONString(lastquery) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                        }
                    } else {
                        if (!lastquery.isEmpty()) {
                            lastdbCollection.deleteMany(lastquery);
                            lastChatLog.info("删除最后一条消息 =====> query " + JSONObject.toJSONString(lastquery) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                        }

                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (null != set) {
                set.close();
            }
        }
    }

    @Override
    public List<Document> queryChatMessageRecord(int userId, int toUserId, long startTime, long endTime, int pageIndex, int pageSize, int maxType) {
        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getMsgRepostory(userId);
        Document q = new Document();
        q.put("sender", userId + "");
        q.put("receiver", toUserId + "");
        if (maxType > 0) {
            q.put("contentType", new Document(MongoOperator.LT, maxType));
        }
        if (0 != startTime && 0 != endTime) {
            startTime = startTime * 1000;
            endTime = endTime * 1000;
            q.put("timeSend", new Document("$gte", startTime).append("$lte", endTime));
        } else if (0 != startTime || 0 != endTime) {
            if (0 != startTime) {
                startTime = startTime * 1000;
                q.put("timeSend", new Document("$gte", startTime));
            } else {
                endTime = endTime * 1000;
                q.put("timeSend", new Document("$lte", endTime));
            }
        }
        q.put("delayedTime", null);

        MongoCursor<Document> iterator = null;
        try {
            iterator = dbCollection.find(q).sort(new Document("timeSend", -1)).skip(pageIndex * pageSize)
                    .limit(pageSize).iterator();
            Document next = null;
            boolean isRead = false;
            while (iterator.hasNext()) {
                next = iterator.next();
                /**
                 * 时间大的已读 其他都设为已读
                 */
                /*if(!isRead) {
                   isRead = 1 == next.getInteger("isRead", 0) ? true : false;
                 }else{
                     next.put("isRead",1);
                }*/
                list.add(next);
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }

    @Override
    public List<Document> queryMucMsgs(String roomJid, long startTime, long endTime, int pageIndex, int pageSize, int maxType, boolean flag) {
        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
        Document q = new Document();
        //q.put("room_jid", roomJid);
        if (0 != startTime && 0 != endTime) {
            if (flag) {
                startTime = startTime * 1000;
//                endTime = endTime * 1000;
            }
            q.put("timeSend", new Document(MongoOperator.GTE, startTime).append(MongoOperator.LTE, endTime));
        } else if (0 != startTime || 0 != endTime) {
            if (0 != startTime) {
                if (flag) {
                    startTime = startTime * 1000;
                }
                q.put("timeSend", new Document(MongoOperator.GTE, startTime));
            } else {
                /*if (flag)
                    endTime = endTime * 1000;*/
                q.put("timeSend", new Document(MongoOperator.LTE, endTime));
            }
        }
        // 群组拉取漫游不返回领取红包消息 contentType=83
        q.put("contentType", new Document(MongoOperator.NE, MessageType.OPENREDPAKET));
        q.put("withdraw", new Document(MongoOperator.NE, "1"));// 过滤撤回的消息
//        q.put("deleteTime", new Document(MongoOperator.LT, DateUtil.currentTimeSeconds()));
        /*
         * DBObject projection=new BasicDBList(); projection.put("body", 1);
         */
        MongoCursor<Document> iterator = null;
        try {
            MongoCollection<Document> collection = mongoTemplate.getCollection(ROOM_MSG_RED);
            iterator = dbCollection.find(q).sort(new Document("timeSend", -1)).skip(pageIndex * pageSize).limit(pageSize).iterator();
            while (iterator.hasNext()) {
                list.add(wrapReadCount(iterator.next(), collection));
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }

    }

    /**
     * 查询用户好友的聊天记录
     *
     * @param sender
     * @param receiver
     * @param page
     * @param limit
     * @return
     */
    @Override
    public PageResult<Document> queryFirendMsgRecord(Integer sender, Integer receiver, Integer page, Integer limit) {
        MongoCollection<Document> dbCollection = getMsgRepostory(sender);
        Document query = new Document();
        if (0 != sender && 0 != receiver) {
            query.append("sender", sender.toString()).append("receiver", receiver.toString());
        } else if (0 != sender) {
            query.append("sender", receiver.toString());
        }
        query.put("contentType", new BasicDBObject(MongoOperator.NE, 202));// 过滤撤回消息

        long total = dbCollection.count(query);


        MongoCursor<Document> cursor = null;
        List<Document> pageData = null;
        PageResult<Document> result = null;
        try {
            cursor = dbCollection.find(query).sort(new BasicDBObject("_id", -1)).skip((page - 1) * limit).limit(limit).iterator();
            pageData = Lists.newArrayList();
            result = new PageResult<Document>();

            while (cursor.hasNext()) {
                Document dbObj = cursor.next();
                @SuppressWarnings("deprecation")
                JSONObject body = JSONObject.parseObject(dbObj.getString("message"));
                if (null != body.get("isEncrypt") && body.getBoolean("isEncrypt")) {
                    dbObj.put("isEncrypt", body.get("encryptType"));
                } else {
                    dbObj.put("isEncrypt", 0);
                }
                if (0 == dbObj.getInteger("direction")) {
                    try {
                        dbObj.put("sender_nickname", userCoreService.getNickName(Integer.valueOf(dbObj.getString("sender"))));
                    } catch (Exception e) {
                        dbObj.put("sender_nickname", "未知");
                    }
                    try {
                        dbObj.put("receiver_nickname", userCoreService.getNickName(Integer.valueOf(dbObj.getString("receiver"))));
                    } catch (Exception e) {
                        dbObj.put("receiver_nickname", "未知");
                    }
                } else {
                    try {
                        dbObj.put("sender_nickname", userCoreService.getNickName(Integer.valueOf(dbObj.getString("receiver"))));
                    } catch (Exception e) {
                        dbObj.put("sender_nickname", "未知");
                    }
                    try {
                        dbObj.put("receiver_nickname", userCoreService.getNickName(Integer.valueOf(dbObj.getString("sender"))));
                    } catch (Exception e) {
                        dbObj.put("receiver_nickname", "未知");
                    }
                }

                try {
                    dbObj.put("content", dbObj.get("content"));
                } catch (Exception e) {
                    dbObj.put("content", "--");
                }
                pageData.add(dbObj);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return result;
        } finally {
            if (null != cursor) {
                cursor.close();
            }

        }

        result.setData(pageData);
        result.setCount(total);
        return result;
    }

    @Override
    public List<Document> queryVisitorMsgRecord(long sender, long receiver, double startTime, int limit) {

        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getMsgRepostory((int) sender);
        Document query = new Document();
        query.put("sender", sender + "");
        query.put("receiver", receiver + "");
        //query.put("withdraw",new Document(MongoOperator.NE,"1"));// 过滤撤回的消息
        if (0 != startTime) {
            query.put("timeSend", new Document("$gt", startTime));
        }

        MongoCursor<Document> iterator = null;
        try {
            Document filter = new Document("message", 1).append("timeSend", 1).append("sender_jid", 1);

            iterator = dbCollection.find(query).projection(filter).sort(new Document("timeSend", -1)).limit(limit).iterator();
            Document next;
            while (iterator.hasNext()) {
                next = iterator.next();

                if (next.getString("sender_jid").contains("/Server")) {
                    /**
                     * 增加系统消息标识
                     */
                    next.put("imSys", 1);
                }

                /**
                 * 替换 酷信的 message 为 body
                 */
                next.put("body", next.get("message"));

                next.remove("message");
                list.add(next);
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }


    @Override
    public List<Document> queryServicerLastChatList(long companyMpId, long serviceId, long startTime, int pageSize) {

        List<Document> resultList = new ArrayList<>();
        MongoCollection<Document> dbCollection = getMongoCollection(lastMsgDB, (int) companyMpId);
        Document query = new Document();
        if (0 != startTime) {
            query.put("timeSend", new Document("$gte", startTime));
        }

        query.append("userId", companyMpId + "");
        query.append("moduleSign", serviceId + "");
        MongoCursor<Document> cursor;
        cursor = dbCollection.find(query).sort(new Document("timeSend", -1)).iterator();
        Document dbObj;
        try {
            while (cursor.hasNext()) {
                dbObj = cursor.next();
                resultList.add(dbObj);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (null != cursor) {
                cursor.close();
            }
        }
        return resultList;
    }

    @Override
    public void deleteMucHistory() {

        //log.info("timeCount  ---> "+(System.currentTimeMillis()-start));
    }

    @Override
    public void dropRoomChatHistory(String roomJid) {
        getImRoomDB().getCollection(MUCMSG + roomJid).drop();
        cleanTigaseMuc_History(roomJid);
        cleanRoomTigase_Nodes(roomJid);

    }

    @Override
    public void cleanTigaseMuc_History(String roomJid) {

    }

    @Override
    public void cleanRoomTigase_Nodes(String roomJid) {
        /*删除 tig_nodes 群组的配置*/

    }

    /**
     * 最后一条消息活跃的群组jid 列表
     *
     * @param page
     * @param limit
     * @return
     */
    @Override
    public PageResult<Document> queryActiveGroupList(int page, int limit) {
        PageResult<Document> result = new PageResult<>();
        Document query = new Document();
        Document sort = new Document("timeSend", -1);

        Document projection = new Document("timeSend", 1);
        projection.put("jid", 1);

        MongoCollection<Document> collection = lastMsgDB.getCollection(DBConstants.LASTCHAT_MUC_COLLECTION);

        result.setCount(collection.countDocuments());

        MongoCursor<Document> iterator = collection.find(query).sort(sort).limit(page * limit).projection(projection).iterator();
        List<Document> dataList = new ArrayList<>();
        Document dbObj;
        try {
            while (iterator.hasNext()) {
                dbObj = iterator.next();
                dataList.add(dbObj);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            iterator.close();
        }
        return result;
    }


    @Override
    public List<Document> queryLastChatList(String userId, long startTime, long endTime, int pageSize, List<String> roomJidList) {
        return queryLastChatList(userId, startTime, endTime, pageSize, roomJidList, 0, null);
    }

    @Override
    public List<Document> queryLastChatList(String userId, long startTime, long endTime, int pageSize, List<String> roomJidList, int needId, List<StickDialog> stickDialogs) {
        List<Document> resultList = new ArrayList<>();
        MongoCollection<Document> dbCollection = getMongoCollection(lastMsgDB, Integer.parseInt(userId));
        Document query = new Document();
        if (0 != startTime && 0 != endTime) {
            query.put("timeSend", new Document("$gte", startTime).append("$lte", endTime));
        }
        if (0 != startTime || 0 != endTime) {
            if (0 != startTime) {
                query.put("timeSend", new Document("$gte", startTime));
            } else {
                query.put("timeSend", new Document("$lte", endTime));
            }
        }

        Document sort = new Document("timeSend", -1);
        MongoCursor<Document> cursor;
        Document dbObj;
        if (null != roomJidList && 0 < roomJidList.size()) {
            // 删除置顶信息
            if (CollectionUtil.isNotEmpty(stickDialogs)) {
                roomJidList.removeAll(stickDialogs.stream().filter(stickDialog -> KConstants.ONE == stickDialog.getIsRoom()).map(StickDialog::getJid).collect(Collectors.toList()));
            }
            query.append("jid", new Document(MongoOperator.IN, roomJidList));
            cursor = lastMsgDB.getCollection(DBConstants.LASTCHAT_MUC_COLLECTION).find(query).sort(sort).limit(pageSize).iterator();
            try {
                while (cursor.hasNext()) {
                    dbObj = cursor.next();
                    resultList.add(dbObj);
                }
            } finally {
                cursor.close();
            }
            query.remove("jid");
        }

        query.append("userId", userId).append("isRoom", 0);
        // 排除指定信息
        if (CollectionUtil.isNotEmpty(stickDialogs)) {
            List<String> userIdList = stickDialogs.stream().filter(stickDialog -> KConstants.ZERO == stickDialog.getIsRoom()).map(StickDialog::getJid).collect(Collectors.toList());
            query.append("jid", new Document(MongoOperator.NIN, userIdList));
        }

        if (0 == pageSize) {
            cursor = dbCollection.find(query).sort(sort).iterator();
        } else {
            cursor = dbCollection.find(query).sort(sort).skip(0).limit(pageSize).iterator();
        }
        try {
            while (cursor.hasNext()) {
                dbObj = cursor.next();
                resultList.add(dbObj);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            cursor.close();
        }
        return KConstants.ONE == needId ? chatListFunction.apply(resultList) : resultList;
    }


    @Override
    public List<Document> queryStickDialogChatList(List<StickDialog> stickDialogs, int needId) {
        if (CollectionUtil.isNotEmpty(stickDialogs)) {
            List<Document> resultList = new ArrayList<>();
            List<String> roomJidList = stickDialogs.stream().filter(stickDialog -> KConstants.ONE == stickDialog.getIsRoom()).map(StickDialog::getJid).collect(Collectors.toList());
            List<String> userIdList = stickDialogs.stream().filter(stickDialog -> KConstants.ZERO == stickDialog.getIsRoom()).map(StickDialog::getJid).collect(Collectors.toList());
            // 查询所有置顶的信息
            MongoCollection<Document> dbCollection = getMongoCollection(lastMsgDB, stickDialogs.get(0).getUserId());
            Document query = new Document("userId", stickDialogs.get(0).getUserId() + "");
            query.append("jid", new Document(MongoOperator.IN, roomJidList));
            // 先查询群组信息
            for (Document document : lastMsgDB.getCollection(DBConstants.LASTCHAT_MUC_COLLECTION).find(query)) {
                resultList.add(document);
            }
            query.replace("jid", new Document(MongoOperator.IN, userIdList));
            for (Document document : dbCollection.find(query)) {
                resultList.add(document);
            }
            // 增加置顶时间
            Map<String, Long> jidMap = stickDialogs.stream().collect(Collectors.toMap(StickDialog::getJid, StickDialog::getTime, (key1, key2) -> key2));
            resultList.forEach(obj -> obj.put("stickTime", jidMap.get(obj.getString("jid"))));
            // 根据置顶时间排序
            resultList.sort((Comparator.comparingLong(o -> o.getLong("stickTime"))));
            return KConstants.ONE == needId ? chatListFunction.apply(resultList) : resultList;
        }
        return Collections.emptyList();
    }

    @Override
    public void delete_type_message_room(Object content, String room_jid, int type) {
        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + room_jid);

        BasicDBObject q = new BasicDBObject();

        if (type == 401) {
            //删除对应 401 消息
            q.put("content", String.valueOf(content));
            q.put("contentType", type);
            dbCollection.deleteMany(q);
        } else if (type == 905) {
            //删除对应 905 消息
            q.put("fileName", String.valueOf(content));
            q.put("contentType", type);
            dbCollection.deleteMany(q);
        } else if (type == 907) {
            q.put("content", String.valueOf(content));
            q.put("contentType", type);
            dbCollection.deleteMany(q);
        }
    }

    @Override
    public PageResult<Document> getMsgRevokeRecordList(long startTime, long endTime, int sender, int receiver, int page, int limit, String keyWord) {
        return null;
    }

    @Override
    public List<Document> querySingleChat(int userId, int toUserId, int pageIndex, int pageSize, long startTime, long endTime, int type, String content) {
        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getMsgRepostory(userId);
        Document q = new Document();
        q.put("sender", userId + "");
        q.put("receiver", toUserId + "");
        if (type != KConstants.ZERO) {
            q.put("contentType", type);
        } else {
            List<Document> queryOr = new ArrayList<>();
            queryOr.add(new Document("contentType", 1));
            queryOr.add(new Document("contentType", 2));
            queryOr.add(new Document("contentType", 3));
            queryOr.add(new Document("contentType", 4));
            queryOr.add(new Document("contentType", 5));
            queryOr.add(new Document("contentType", 6));
            queryOr.add(new Document("contentType", 7));
            queryOr.add(new Document("contentType", 8));
            queryOr.add(new Document("contentType", 9));
            queryOr.add(new Document("contentType", 82));
            queryOr.add(new Document("contentType", 84));
            queryOr.add(new Document("contentType", 85));
            queryOr.add(new Document("contentType", 94));
            q.append(MongoOperator.OR, queryOr);
        }
        if (!StringUtil.isEmpty(content)) {
            q.append("content", new BasicDBObject(MongoOperator.REGEX, content));
        }
        if (0 != startTime) {
            startTime = startTime;
            q.put("timeSend", new Document("$lte", startTime));
        }
        // 过滤撤回的消息
        q.put("delayedTime", null);
        MongoCursor<Document> iterator = null;
        try {
            iterator = dbCollection.find(q).sort(new Document("timeSend", -1)).skip(pageIndex * pageSize)
                    .limit(pageSize).iterator();
            Document next = null;
            boolean isRead = false;
            while (iterator.hasNext()) {
                next = iterator.next();
                list.add(next);
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }

    @Override
    public List<Document> queryGroupChat(String roomJid, int pageIndex, int pageSize, long startTime, long endTime, int type, String content, boolean flag, int memberId) {
        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
        Document q = new Document();
        if (0 != startTime) {
            q.put("timeSend", new Document(MongoOperator.LTE, startTime));
        }
        if (type != 0) {
            q.put("contentType", type);
        } else {
            List<Document> queryOr = new ArrayList<>();
            queryOr.add(new Document("contentType", 1));
            queryOr.add(new Document("contentType", 2));
            queryOr.add(new Document("contentType", 3));
            queryOr.add(new Document("contentType", 4));
            queryOr.add(new Document("contentType", 5));
            queryOr.add(new Document("contentType", 6));
            queryOr.add(new Document("contentType", 7));
            queryOr.add(new Document("contentType", 8));
            queryOr.add(new Document("contentType", 9));
            queryOr.add(new Document("contentType", 82));
            queryOr.add(new Document("contentType", 84));
            queryOr.add(new Document("contentType", 85));
            queryOr.add(new Document("contentType", 94));
            q.append(MongoOperator.OR, queryOr);
        }
        if (memberId != 0) {
            q.put("sender", memberId + "");
        }
        if (!StringUtil.isEmpty(content)) {
            q.append("content", new BasicDBObject(MongoOperator.REGEX, content));
        }
        // 过滤撤回的消息
        q.put("delayedTime", null);
        MongoCursor<Document> iterator = null;
        try {

            MongoCollection<Document> collection = getImRoomDB().getCollection(MUCMSG + roomJid);
            ;
            iterator = dbCollection.find(q).sort(new Document("timeSend", -1)).skip(pageIndex * pageSize)
                    .limit(pageSize).iterator();
            while (iterator.hasNext()) {
                list.add(wrapReadCount(iterator.next(), collection));
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }

    }

    @Override
    public void groupchat_logs_all_edit(String messageId, String room_jid, String content) {
        try {
            MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + room_jid);
            Document query = new Document();
            query.put("messageId", messageId);

            Document dbObj = dbCollection.find(query).first();
            String body;
            if (null == dbObj) {
                return;
            } else {
                body = dbObj.getString("message");
                if (null == body) {
                    return;
                }
            }
            // 解析消息体
            Map<String, Object> msgBody = JSON.parseObject(body);
            msgBody.put("content", content);
            body = JSON.toJSON(msgBody).toString();
            Document values = new Document();
            values.put("message", body);
            values.put("content", content);
            dbCollection.updateMany(Filters.eq("messageId", messageId), new Document(MongoOperator.SET, values));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateMessageContent(MongoCollection<Document> dbCollection, Document query, String content) {
        Document dbObj = dbCollection.find(query).first();
        String body;

        Document values = new Document();
        values.put("content", content);
        values.put("type", MsgType.TYPE_EDIT);

        if (null == dbObj) {
            return;
        } else {
            body = dbObj.getString("message");
            if (null != body) {
                Map<String, Object> msgBody = JSON.parseObject(body);
                msgBody.put("content", content);
                body = JSON.toJSON(msgBody).toString();
                values.put("message", body);
            }
        }
        // 解析消息体
        dbCollection.updateMany(query, new Document(MongoOperator.SET, values));
        lastChatLog.info("改变最后一条消息记录 =====> query " + JSONObject.toJSONString(query) + " 消息：" + JSONObject.toJSONString(values));
    }

    @Override
    public void editMsgUpdateLastMessage(int sender, int toUserId, String roomJid, String messageId, int type, String message) {
        /**
         type 聊天类型 1 单聊  2 群聊
         */
        DBCursor cursor = null;
        MongoCollection<Document> dbCollection;
        MongoCollection<Document> lastdbCollection = null;

        try {
            Document query = new Document();
            query.put("messageId", messageId);

            if (type == 1) {
                dbCollection = getMsgRepostory(sender);
                lastdbCollection = getMongoCollection(lastMsgDB, sender);
            } else {
                dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
                lastdbCollection = getLastMsgDB().getCollection(DBConstants.LASTCHAT_MUC_COLLECTION);
            }
            updateMessageContent(dbCollection, query, message);
            updateMessageContent(lastdbCollection, query, message);
            if (type == 1) {
                //                接收用户消息更新
                dbCollection = getMsgRepostory(toUserId);
                lastdbCollection = getMongoCollection(lastMsgDB, toUserId);

                updateMessageContent(dbCollection, query, message);
                updateMessageContent(lastdbCollection, query, message);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public List<Document> queryLastChatList(int userId, long startTime, int pageSize) {
        List<Document> resultList = Lists.newArrayList();
        BasicDBObject query = new BasicDBObject();
        if (0 != startTime) {
            query.put("timeSend", new BasicDBObject("$lt", startTime));
        }
        query.append("userId", userId + "");
        Document dbObj;
        try (MongoCursor<Document> cursor = getMongoCollection(lastMsgDB, userId).find(query).sort(new Document("timeSend", -1)).limit(pageSize).iterator()) {
            while (cursor.hasNext()) {
                dbObj = cursor.next();
                if ((int) dbObj.get("isRoom") != 1) {
                    String nickName = userCoreService.getNickName(Integer.parseInt((String) dbObj.get("jid")));
                    dbObj.put("toUserName", nickName);
                }
                resultList.add(dbObj);
            }
        }
        return resultList;
    }

    @Override
    public List<Document> pullMessageBySeqNos(long userId, long toUserId, Set<Long> seqNos) {

        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getMongoCollection(chatMsgDB, (int) userId);
        Document query = new Document();
        query.put("sender", userId + "");
        query.put("receiver", toUserId + "");

        query.append("seqNo", new Document("$in", seqNos));
        query.put("delayedTime", null);
        MongoCursor<Document> iterator = null;
        try {
            iterator = dbCollection.find(query).iterator();
            Document next;
            while (iterator.hasNext()) {
                next = iterator.next();
                list.add(next);
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }

    @Override
    public List<Document> pullGroupMessageBySeqNos(String roomJid, Set<Long> seqNoSets) {
        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = imRoomDB.getCollection(MUCMSG + roomJid);
        Document query = new Document();
        query.put("room_jid", roomJid);
        query.append("seqNo", new Document("$in", seqNoSets));
        query.put("delayedTime", null);
        MongoCursor<Document> iterator = null;
        MongoCollection<Document> collection = mongoTemplate.getCollection(ROOM_MSG_RED);
        try {
            iterator = dbCollection.find(query).iterator();
            Document next;
            while (iterator.hasNext()) {
                next = iterator.next();
                list.add(wrapReadCount(next, collection));
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }

    @Override
    public List<Document> queryMultipointChat(Integer userId, long startTime, long endTime, String from, String to, int pageSize) {
        return null;
    }

    @Override
    public List<? extends Object> queryRoomMessageReadList(int userId, String roomJid, String messageId, int isRead, int pageIndex, int pageSize) {
        // 查询 tigase 添加到 imapi 数据库中的已读信息
        MongoCollection<Document> roomMsgRead = mongoTemplate.getCollection(ROOM_MSG_RED);
        Document query = new Document();
        query.append("roomJid", roomJid);
        query.append("messageId", messageId);
        FindIterable<Document> documents = roomMsgRead.find(query);
        MongoCursor<Document> cursor = documents.iterator();
        Document document;
        try {
            // 根据查询状态查询已读列表或者未读列表
            while (cursor.hasNext()) {
                document = cursor.next();
                List<Integer> userIds = document.getList("redUsers", Integer.class);
                if (CollectionUtil.isNotEmpty(userIds)) {
                    if (KConstants.ZERO == isRead) {
                        if (userIds.contains(userId)) {
                            if (userIds.size() == 1) {
                                return null;
                            }
                            userIds.remove(userId); // 排除当前用户
                        }
                    } else {
                        if (!userIds.contains(userId)) {
                            userIds.add(userId); // 排除当前用户
                        }
                    }
                    if (KConstants.ZERO == isRead) {
                        return wrapReadUser(userCoreService.findUserByIds(userIds, pageIndex, pageSize));
                    } else {
                        return roomManagerImpl.findByNotInUserIds(roomJid, userIds, pageIndex, pageSize);
                    }
                }
            }
        } finally {
            if (null != cursor) {
                cursor.close();
            }
        }

        // 如果查询不到，说明全部未读，直接返回
        return KConstants.ZERO == isRead ? Collections.emptyList() : roomManagerImpl.findMemberByRoomJid(userId, roomJid, pageIndex, pageSize);
    }

    @Override
    public long queryRoomMessageReadCount(Integer verifyUserId, String roomJid, String messageId) {
        MongoCollection<Document> roomMsgRead = mongoTemplate.getCollection(ROOM_MSG_RED);
        Document query = new Document();
        query.append("roomJid", roomJid);
        query.append("messageId", messageId);
        Document document = roomMsgRead.find(query).first();
        if (null == document) {
            return 0;
        }
        List<Integer> userIds = document.getList("redUsers", Integer.class);
        if (null == userIds) {
            return 0;
        }
        return userIds.size();
    }


    private List<ReadUserDTO> wrapReadUser(List<User> users) {
        if (CollectionUtil.isNotEmpty(users)) {
            return users.stream().map(user -> new ReadUserDTO().setUserId(user.getUserId())
                    .setNickname(user.getNickname())
                    .setHiding(ObjectUtil.isNotNull(user.getSettings()) ? user.getSettings().getHiding() : KConstants.ZERO)).collect(Collectors.toList());
        }
        return null;
    }


    @Override
    public List<ReadDTO> queryRoomMessageReadLastTime(Long lastTime, Integer userId) {
        // 查询 tigase 添加到 imapi 数据库中的已读信息
        MongoCollection<Document> roomMsgRead = mongoTemplate.getCollection(ROOM_MSG_RED);
        Document query = new Document();
        query.append("userId", userId);
        query.append("modifyTime", new Document("$gte", lastTime)); // 修改时间大于用户最后一次登录时间的数据
        MongoCursor<Document> cursor = null;
        /**
         * 2019-01-01
         *
         */
        if (1546315200 < lastTime) {
            cursor = roomMsgRead.find(query).iterator();
        } else {
            cursor = roomMsgRead.find(query).sort(new Document("modifyTime", -1)).limit(100).iterator();
        }
        Map<String, List<Document>> unReadMap = new HashMap<>();
        Document document;
        try {
            // 根据 roomJid 分组
            while (cursor.hasNext()) {
                document = cursor.next();
                String roomJid = document.getString("roomJid");
                List<Document> documentList = unReadMap.computeIfAbsent(roomJid, k -> new ArrayList<>());
                documentList.add(document);
            }
        } finally {
            if (null != cursor) {
                cursor.close();
            }
        }

        // 处理为 UnReadDTO 实体类对象
        List<ReadDTO> readDTOS = new ArrayList<>();
        unReadMap.forEach((roomJid, docs) -> {
            List<ReadDTO.Message> messageList = new ArrayList<>();
            docs.forEach(obj -> {
                ReadDTO.Message message = new ReadDTO.Message();
                message.setMessageId(obj.getString("messageId"));
                List<Integer> userIds = obj.getList("redUsers", Integer.class);
                message.setCount(null != userIds ? userIds.size() : KConstants.ZERO);
                messageList.add(message);
            });
            ReadDTO readDTO = new ReadDTO();
            readDTO.setRoomJid(roomJid);
            readDTO.setMessageList(messageList);
            readDTOS.add(readDTO);
        });
        return readDTOS;
    }


    @Autowired
    private AuthKeysServiceImpl authKeysService;


    @Override
    public JSONObject queryChatDetails(Integer userId, List<Integer> userIds, List<String> roomIds) {
        JSONObject result = new JSONObject();
        // 查询用户信息
        if (CollectionUtil.isNotEmpty(userIds)) {
            List<User> userList = new ArrayList<>();
            userIds.forEach(toUserId -> {
                User user = userManager.getUser(toUserId);
                if (ObjectUtil.isNotNull(user)) {
                    userCoreService.wrapUser(user);
                    //查找用户公私钥
                    Optional<AuthKeys> authKeys = Optional.ofNullable(authKeysService.getAuthKeys(toUserId));
                    if (authKeys.isPresent()) {
                        user.setDhMsgPublicKey((authKeys.get().getMsgDHKeyPair() != null) ? authKeys.get().getMsgDHKeyPair().getPublicKey() : null);
                        user.setRsaMsgPublicKey((authKeys.get().getMsgRsaKeyPair() != null) ? authKeys.get().getMsgRsaKeyPair().getPublicKey() : null);
                    }
                    userList.add(user);
                }
            });
            result.put("users", userList);
        }
        // 查询群组信息
        if (CollectionUtil.isNotEmpty(roomIds)) {
            List<Room> roomList = new ArrayList<>();
            roomIds.forEach(roomId -> {
                if (ObjectId.isValid(roomId)) {
                    try {
                        ObjectId roomObjId = new ObjectId(roomId);
                        Room room = roomManagerImpl.getRoom(roomObjId);
                        room.setMember(roomManagerImpl.getMember(roomObjId, ReqUtil.getUserId()));
                        if (room.getUserSize() > 50) {
                            room.setShowRead((byte) 0);
                        }
                        roomList.add(room);
                    } catch (ServiceException e) {
                        logger.info("{} not found", roomId);
                    }
                }
            });
            result.put("rooms", roomList);
        }
        return result;
    }

    @Override
    public Set<String> findTigaseDocuments() {
        return null;
    }

    @Override
    public Set<String> findChatMsgsDocuments() {
        Set<String> collectionNames = mongoTemplateForChatMsgs.getCollectionNames();
        return collectionNames;
    }

    @Override
    public List<Document> queryChatMessageBySeqNo(long userId, long toUserId, long startSeqNo, long endSeqNo, int pageIndex, int pageSize) {

        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = getMongoCollection(chatMsgDB, (int) userId);
        Document query = new Document();
        query.put("sender", userId + "");
        query.put("receiver", toUserId + "");

        if (startSeqNo > KConstants.ZERO && endSeqNo > KConstants.ZERO) {
            query.append("seqNo", new Document(MongoOperator.GT, startSeqNo).append(MongoOperator.LT, endSeqNo));
        } else if (startSeqNo > KConstants.ZERO || endSeqNo > KConstants.ZERO) {
            if (startSeqNo > KConstants.ZERO) {
                query.append("seqNo", new Document(MongoOperator.GT, startSeqNo));
            } else {
                query.append("seqNo", new Document(MongoOperator.GT, startSeqNo).append(MongoOperator.LT, endSeqNo));
            }

        } else {
            query.append("seqNo", new Document(MongoOperator.GT, startSeqNo));
        }
        //query.append("seqNo", new Document(MongoOperator.GT, startSeqNo).append(MongoOperator.LT, endSeqNo));


        query.put("delayedTime", null);
        MongoCursor<Document> iterator = null;
        try {
            iterator = dbCollection.find(query).sort(new Document("timeSend", -1)).skip(pageIndex * pageSize)
                    .limit(pageSize).iterator();
            Document next;
            while (iterator.hasNext()) {
                next = iterator.next();
                list.add(next);
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }

    @Override
    public List<Document> queryGroupMessageBySeqNo(String roomJid, long startSeqNo, long endSeqNo, int pageIndex, int pageSize) {
        List<Document> list = Lists.newArrayList();
        MongoCollection<Document> dbCollection = imRoomDB.getCollection(MUCMSG + roomJid);
        Document query = new Document();
        query.put("room_jid", roomJid);
        if (startSeqNo > KConstants.ZERO && endSeqNo > KConstants.ZERO) {
            query.append("seqNo", new Document(MongoOperator.GT, startSeqNo).append(MongoOperator.LT, endSeqNo));
        } else if (startSeqNo > KConstants.ZERO || endSeqNo > KConstants.ZERO) {

            query.append("seqNo", new Document(MongoOperator.GT, startSeqNo).append(MongoOperator.LT, endSeqNo));
        } else {
            query.append("seqNo", new Document(MongoOperator.GT, startSeqNo));
        }
        query.put("delayedTime", null);
        MongoCursor<Document> iterator = null;
        try {
            iterator = dbCollection.find(query).sort(new Document("timeSend", -1)).skip(pageIndex * pageSize).limit(pageSize).iterator();
            MongoCollection<Document> collection = mongoTemplate.getCollection(ROOM_MSG_RED);
            Document next;
            while (iterator.hasNext()) {
                next = iterator.next();
                list.add(wrapReadCount(next, collection));
            }
            return list;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return list;
        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }


    @Override
    public void deleteTimeOutChatMsgRecord() {
        MongoCollection<Document> dbCollection = null;
        MongoCollection<Document> lastdbCollection = null;
        MongoCursor<String> dbNames = null;
        try {
            logger.info("=====> deleteChatMsgRecord " + DateUtil.TimeToStr(new Date()));


            // 未读的撤回消息
            BasicDBList withdrawQueryOr = new BasicDBList();
            withdrawQueryOr.add(new BasicDBObject("isRead", 1));
            withdrawQueryOr.add(new BasicDBObject("isRead", 0).append("delayedTime", new BasicDBObject(MongoOperator.LT, DateUtil.currentTimeSeconds())));

            Document query = new Document();
            query.append(MongoOperator.OR, withdrawQueryOr);
            query.append("deleteTime", new Document(MongoOperator.GT, 0)
                    .append(MongoOperator.LT, DateUtil.currentTimeSeconds()));

            dbNames = chatMsgDB.listCollectionNames().iterator();
            String name = null;
            while (dbNames.hasNext()) {
                name = dbNames.next();
                dbCollection = chatMsgDB.getCollection(name);
                lastdbCollection = lastMsgDB.getCollection(name);
                deleteTimeOutChatMsgRecord(dbCollection, lastdbCollection, query);
            }


        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (null != dbNames) {
                dbNames.close();
            }
        }
    }

    public void deleteTimeOutChatMsgRecord(MongoCollection<Document> dbCollection, MongoCollection<Document> lastdbCollection, Document query) {
        try {

            //long count = dbCollection.count(query);
            Document base = dbCollection.find(query).first();
            //"chat_msgs.1000".equals(dbCollection.getNamespace())
            Document lastquery = new Document();
            BasicDBList queryOr = new BasicDBList();
            if (base != null) {
                queryOr.add(new BasicDBObject("jid", String.valueOf(base.get("sender"))).append("userId", base.get("receiver").toString()));
                queryOr.add(new BasicDBObject("userId", String.valueOf(base.get("sender"))).append("jid", base.get("receiver").toString()));
                lastquery.append(MongoOperator.OR, queryOr);
            } else {
                return;
            }

            // 删除文件
            query.append("contentType", new BasicDBObject(MongoOperator.IN, MsgType.FileTypeArr));
            MongoCursor<String> iterator = dbCollection.distinct("content", query, String.class).iterator();

            while (iterator.hasNext()) {
                ConstantUtil.deleteFile(iterator.next());
            }
            if (null != iterator) {
                iterator.close();
            }
            //将消息记录中的数据删除
            dbCollection.deleteMany(query);

            logger.info("=====> deleteTimeOutChatMsgRecord {}  {}", dbCollection.getNamespace(), query.toJson());

            query.remove("contentType");


            //将消息记录中的数据删除
            dbCollection.deleteMany(query);


            /**
             * 这一块有重大bug 先注释掉
             */
            /*query.remove("messageId");
            query.remove("sender");

            // 重新查询一条消息记录插入
            BasicDBList baslist = new BasicDBList();
            if (base != null) {
                baslist.add(new BasicDBObject("receiver", base.get("sender")));
                baslist.add(new BasicDBObject("sender", base.get("sender")));
                query.append(MongoOperator.OR, baslist);
            }
            query.remove("sender");
            query.remove("deleteTime");
            query.remove("isRead");
            Document lastMsgObj = dbCollection.find(query).sort(new BasicDBObject("timeSend", -1)).limit(1).first();

            if (lastMsgObj != null) {
                BasicDBObject values = new BasicDBObject();
                values.put("messageId", lastMsgObj.get("messageId"));
                values.put("timeSend", new Double(lastMsgObj.get("timeSend").toString()).longValue());
                values.put("content", lastMsgObj.get("content"));
                if (!lastquery.isEmpty()) {
                    lastdbCollection.updateMany(lastquery, new BasicDBObject(MongoOperator.SET, values));
                    lastChatLog.info("改变最后一条消息记录 =====> query " + JSONObject.toJSONString(lastquery) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
                }
            } else {
                if(!lastquery.isEmpty()){
                    lastdbCollection.deleteMany(lastquery);
                    lastChatLog.info("删除最后一条消息 =====> query "+JSONObject.toJSONString(lastquery) +" 当前方法 "+ StringUtils.getCurrentMethod()+" 方法调用路线 "+StringUtils.getMethoedPath());
                }
            }*/

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void deleteMsgUpdateLastMessage(final int sender, int toUserId, String roomJid, final String messageId
            , int delete, int type) {
        /**
         type 聊天类型 1 单聊  2 群聊
         delete 1： 删除属于自己的消息记录 2：撤回 删除整条消息记录
         */
        DBCursor cursor = null;
        MongoCollection<Document> dbCollection;
        MongoCollection<Document> lastdbCollection = null;
        try {
            if (type == 1) {
                dbCollection = getMsgRepostory(sender);
                // 最后一条聊天消息
                lastdbCollection = getMongoCollection(lastMsgDB, sender);
            } else {
                dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
                lastdbCollection = getLastMsgDB().getCollection(DBConstants.LASTCHAT_MUC_COLLECTION);
            }

            Document query = new Document();
            if (!StringUtil.isEmpty(messageId)) {
                String[] split = messageId.split(",");
                query.put("messageId", new Document(MongoOperator.IN, Arrays.asList(split)));
            } else {
                logger.info(" messageId is null ===>");
                return;
            }
            if (1 == delete) {
                query.put("sender", sender + "");
            }
            int delayedTime = SKBeanUtils.getSystemConfig().getMsgDelayedDeleteTime();

            /**
             * 清除聊天记录接口里，不删除文件
             */
            Document base = dbCollection.find(query).first();
            if (null == base) {
                return;
            }
            String receiver = base.getString("receiver");
            Document lastquery = new Document();
            // 维护最后一条消息记录表
            BasicDBList queryOr = new BasicDBList();
            if (1 == type) {
                if (delete == 1) {
                    lastquery.put("userId", sender + "");
                    query.append("sender", sender + "");
                    if (0 != toUserId) {
                        lastquery.append("jid", toUserId + "");
                    } else if (!StringUtil.isEmpty(messageId)) {
                        String[] split = messageId.split(",");
                        lastquery.put("messageId", new Document(MongoOperator.IN, Arrays.asList(split)));
                    }
                } else if (delete == 2) {
                    // 单聊下用户是否开启撤回消息物理删除
                    query.append(MongoOperator.OR,
                            Arrays.asList(new Document("sender", sender + ""), new Document("receiver", sender + "")));
                    if (0 == delayedTime) {
                        query.append("contentType", new BasicDBObject(MongoOperator.IN, MsgType.FileTypeArr));
                        MongoCursor<String> iterator = dbCollection.distinct("content", query, String.class).iterator();
                        while (iterator.hasNext()) {
                            // 调用删除方法将文件从服务器删除
                            ConstantUtil.deleteFile(iterator.next());
                        }
                        if (null != iterator) {
                            iterator.close();
                        }
                        query.remove("contentType");
                    }
                    if (null != base.get("sender") && null != base.get("receiver")) {
                        queryOr.add(new BasicDBObject("jid", String.valueOf(base.get("sender"))).append("userId",
                                base.get("receiver").toString()));
                        queryOr.add(new BasicDBObject("userId", String.valueOf(base.get("sender"))).append("jid",
                                base.get("receiver").toString()));
                        lastquery.append(MongoOperator.OR, queryOr);
                    }
                }
            } else {
                lastquery.put("jid", roomJid);
                query.put("room_jid", roomJid);
            }

            // 撤回消息设标志位

            if (0 < delayedTime) {
                if (1 == delete) {
                    // 添加消息标志位，拉取漫游过滤
                    long currentTime = DateUtil.currentTimeSeconds();
                    Document withdrawVal = new Document();

                    withdrawVal.put("delayedTime", currentTime + delayedTime);

                    withdrawVal.put("deleteTime", currentTime + delayedTime);
                   /* if (2 == type) {
                        withdrawVal.put("withdrawName", userCoreService.getNickName(sender));// 撤回人昵称(群聊可能不是自己撤回)
                    }*/
                    dbCollection.updateMany(query, new Document(MongoOperator.SET, withdrawVal));
                } else {
                    long currentTime = DateUtil.currentTimeSeconds();
                    Document withdrawVal = new Document();

                    withdrawVal.put("delayedTime", currentTime + delayedTime);

                    withdrawVal.put("deleteTime", currentTime + delayedTime);

                    dbCollection.updateMany(query, new Document(MongoOperator.SET, withdrawVal));
                    if (1 == type && 2 == delete && null != receiver) {
                        dbCollection = getMongoCollection(chatMsgDB, Integer.parseInt(receiver));
                        dbCollection.updateMany(query, new Document(MongoOperator.SET, withdrawVal));
                        dbCollection.deleteMany(query);
                    }

                }
            } else {
                // 删除单聊离线消息
                //delOfficeMsg(messageId);
                dbCollection.deleteMany(query);
                if (1 == type && 2 == delete && null != receiver) {
                    dbCollection = getMongoCollection(chatMsgDB, Integer.parseInt(receiver));
                    dbCollection.deleteMany(query);
                }
            }

            query.remove("messageId");
            if (null != receiver) {
                query.put("receiver", receiver + "");
            }
            query.put("delayedTime", null);

            Document lastMsgObj = dbCollection.find(query).sort(new BasicDBObject("timeSend", -1)).limit(1).first();
            if (null == lastMsgObj) {
                return;
            }
            Document values = new Document();
            values.put("messageId", lastMsgObj.get("messageId"));
            values.put("timeSend", new Double(lastMsgObj.get("timeSend").toString()).longValue());
            values.put("content", lastMsgObj.get("content"));
            lastdbCollection.updateMany(lastquery, new Document(MongoOperator.SET, values));
            if (1 == type && 2 == delete && null != receiver) {
                lastquery.replace("userId", receiver);
                lastdbCollection = getMongoCollection(lastMsgDB, Integer.parseInt(receiver));
                lastdbCollection.updateMany(lastquery, new Document(MongoOperator.SET, values));
                lastChatLog.info("改变最后一条消息记录 =====> query " + JSONObject.toJSONString(lastquery) + " 当前方法 " + StringUtils.getCurrentMethod() + " 方法调用路线 " + StringUtils.getMethoedPath());
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public void changeMsgReadStatus(String messageId, int userId, int toUserId) {
        try {
            MongoCollection<Document> dbCollection = getMsgRepostory(userId);

            Document query = new Document();
            query.put("messageId", messageId);

            Document dbObj = dbCollection.find(query).first();
            String body;
            if (null == dbObj) {
                return;
            } else {
                body = dbObj.getString("message");
                if (null == body) {
                    return;
                }
            }
            // 解析消息体
            Map<String, Object> msgBody = JSON.parseObject(body);
            msgBody.put("isRead", 1);
            body = JSON.toJSON(msgBody).toString();
            dbCollection.updateMany(query, new Document(MongoOperator.SET, new BasicDBObject("message", body)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public PageResult<Document> chat_logs_all(long startTime, long endTime, int sender,
                                              int receiver, int page, int limit, String keyWord, String documents) throws Exception {
        MongoCollection<Document> dbCollection;
        if (!StringUtil.isEmpty(documents)) {
            dbCollection = getChatMsgDB().getCollection(documents);
        } else {
            dbCollection = getChatMsgDB().getCollection("10");
        }
        BasicDBObject q = new BasicDBObject();
        if (0 == receiver) {
            q.put("receiver", new BasicDBObject("$ne", 10005 + ""));
            q.put("direction", 0);
        } else {
            q.put("direction", 0);
            q.put("receiver", BasicDBObjectBuilder.start("$eq", receiver + "").add("$ne", 10005 + "").get());
        }
        if (0 == sender) {
            q.put("sender", new BasicDBObject("$ne", 10005 + ""));
            q.put("direction", 0);
        } else {
            q.put("direction", 0);
            q.put("sender", BasicDBObjectBuilder.start("$eq", sender + "").add("$ne", 10005 + "").get());
        }
        if (!StringUtil.isEmpty(keyWord)) {
            q.put("content", new BasicDBObject(MongoOperator.REGEX, keyWord));
        }

        if (0 != startTime) {
            q.put("ts", new BasicDBObject("$gte", startTime));
        }
        if (0 != endTime) {
            q.put("ts", new BasicDBObject("$lte", endTime));
        }
        q.put("contentType", new BasicDBObject(MongoOperator.NE, 202));// 过滤撤回消息
        q.put("isReadDel", new Document(MongoOperator.NE, 1));// 过滤阅后即焚消息
        long total = dbCollection.count(q);
        List<Document> pageData = Lists.newArrayList();

        MongoCursor<Document> cursor = dbCollection.find(q).sort(new BasicDBObject("_id", -1)).skip((page - 1) * limit).limit(limit).iterator();
        PageResult<Document> result = new PageResult<Document>();
        while (cursor.hasNext()) {
            Document dbObj = cursor.next();
            JSONObject body = JSONObject.parseObject(dbObj.getString("message"));
            if (null == body)
                continue;
           /* if (null != body.get("isEncrypt") && body.getBoolean("isEncrypt")) {
                dbObj.put("isEncrypt", body.get("encryptType"));
            } else {
                dbObj.put("isEncrypt", 0);
            }*/

            dbObj.put("encryptType", body.get("encryptType"));

            try {
                dbObj.put("sender_nickname", userCoreService.getNickName(Integer.parseInt(dbObj.getString("sender"))));
            } catch (Exception e) {
                dbObj.put("sender_nickname", "未知");
            }
            try {
                dbObj.put("receiver_nickname", userCoreService.getNickName(Integer.valueOf(dbObj.getString("receiver"))));
            } catch (Exception e) {
                dbObj.put("receiver_nickname", "未知");
            }
            try {
                dbObj.put("content", dbObj.get("content"));
            } catch (Exception e) {
                dbObj.put("content", "--");
            }

            pageData.add(dbObj);

        }
        result.setData(pageData);
        result.setCount(total);
        return result;
    }

    @Override
    public long chat_logs_all_count(long startTime, long endTime, int sender, int receiver, int page, int limit, String keyWord, String documents) throws Exception {

        return 0;
    }


    @Override
    public void chat_logs_all_del(long startTime, long endTime, int sender,
                                  int receiver, int pageIndex, int pageSize) throws Exception {

        MongoCollection<Document> dbCollection = getMsgRepostory(sender);
        Document q = new Document();

        if (0 == sender) {
            q.put("sender", new Document("$ne", 10005));
        } else {
            q.put("sender", BasicDBObjectBuilder.start("$eq", sender).add("$ne", 10005).get());
        }
        if (0 == receiver) {
            q.put("receiver", new BasicDBObject("$ne", 10005));
        } else {
            q.put("receiver", BasicDBObjectBuilder.start("$eq", receiver).add("$ne", 10005).get());
        }
        if (0 != startTime) {
            q.put("ts", new Document("$gte", startTime));
        }
        if (0 != endTime) {
            q.put("ts", new Document("$lte", endTime));
        }
        dbCollection.deleteMany(q);

    }

    /*@Override
    public void deleteChatMsgs(String msgId, int type) {

    }*/


    public void deleteChatMsgs(String msgId, int type, String collectionName) {

        MongoCollection<Document> dbCollection;

        if (StringUtil.isEmpty(collectionName)) {
            dbCollection = chatMsgDB.getCollection("1000");
        } else {
            dbCollection = chatMsgDB.getCollection(collectionName);
        }

        BasicDBObject q = new BasicDBObject();
        try {
            if (0 == type) {
                String[] msgIds = StringUtil.getStringList(msgId);
                for (String strMsgId : msgIds) {
                    q.put("_id", new ObjectId(strMsgId));
                    dbCollection.deleteMany(q);
                }
            } else if (1 == type) {
                // 删除一个月前的聊天记录
                long onedayNextDay = DateUtil.getOnedayNextDay(DateUtil.currentTimeSeconds(), 30, 1);
                System.out.println("上个月的时间：" + onedayNextDay);
                q.put("timeSend", new BasicDBObject("$lte", onedayNextDay));
                dbCollection.deleteMany(q);
            } else if (2 == type) {
                final long num = 100000;
                long count = dbCollection.countDocuments();
                if (count <= num)
                    throw new ServiceException("数量小于等于" + num);
                // 删除十万条前的聊天记录
                FindIterable<Document> timeSend = dbCollection.find().sort(new BasicDBObject("timeSend", -1)).skip((int) num).limit(1);
                Document document = timeSend.first();
                Long timeSendKey = document.getLong("timeSend");
                Document query = new Document();
                query.put("timeSend", new Document(MongoOperator.LTE, timeSendKey));
                dbCollection.deleteMany(query);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void changeMsgContent(String messageId, Integer sender, Integer receiver, String content) {
        MongoCollection<Document> dbCollection;
        try {
            for (int user : Arrays.asList(sender, receiver)) {
                dbCollection = chatMsgDB.getCollection(getCollectionName(user));
                Document query = new Document();
                query.put("messageId", messageId);

                Document dbObj = dbCollection.find(query).first();
                String body;
                if (null == dbObj) {
                    return;
                } else {
                    body = dbObj.getString("message");
                    if (null == body) {
                        return;
                    }
                }
                // 解析消息体
                Map<String, Object> msgBody = JSON.parseObject(body);
                msgBody.put("content", content);
                body = JSON.toJSON(msgBody).toString();
                Document values = new Document();
                values.put("message", body);
                values.put("content", content);
                dbCollection.updateMany(Filters.eq("messageId", messageId), new Document(MongoOperator.SET, values));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public PageResult<Document> groupchat_logs_all(long startTime, long endTime, String room_jid,
                                                   int page, int limit, String keyWord, String sender) {
        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + room_jid);

        BasicDBObject q = new BasicDBObject();
        if (0 != startTime) {
            q.put("ts", new BasicDBObject("$gte", startTime));
        }
        if (0 != endTime) {
            q.put("ts", new BasicDBObject("$lte", endTime));
        }
        if (!StringUtil.isEmpty(keyWord)) {
            q.put("content", new BasicDBObject(MongoOperator.REGEX, keyWord));
        }
        if (!StringUtil.isEmpty(sender)) {
            q.put("sender", sender + "");
        }
        q.put("contentType", new BasicDBObject(MongoOperator.NE, 202));// 过滤撤回消息
        long total = dbCollection.count(q);
        List<Document> pageData = Lists.newArrayList();
        PageResult<Document> result = new PageResult<Document>();
        MongoCursor<Document> cursor = dbCollection.find(q).sort(new BasicDBObject("ts", -1)).skip((page - 1) * limit).limit(limit).iterator();
        while (cursor.hasNext()) {
            Document dbObj = cursor.next();
            @SuppressWarnings("deprecation")
            JSONObject body;
            try {
                body = JSONObject.parseObject(dbObj.getString("message"));
            } catch (Exception e) {
                body = null;
            }
            if (null == body) {
                continue;
            }
            if (null != body.get("isEncrypt") && body.getBoolean("isEncrypt")) {
                dbObj.put("isEncrypt", 1);
            } else {
                dbObj.put("isEncrypt", 0);
            }
            try {
                dbObj.put("content", body.getString("content"));
                dbObj.put("fromUserName", body.get("fromUserName"));
            } catch (Exception e) {
                dbObj.put("content", "--");
            }
            pageData.add(dbObj);

        }

        result.setData(pageData);
        result.setCount(total);
        return result;
    }

    @Override
    public void groupchat_logs_all_del(long startTime, long endTime,
                                       String msgId, String room_jid) throws Exception {

        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + room_jid);

        BasicDBObject q = new BasicDBObject();

        String[] msgIds = StringUtil.getStringList(msgId);
        for (String strMsgId : msgIds) {
            q.put("_id", new ObjectId(strMsgId));
            dbCollection.deleteMany(q);
        }

        if (0 != startTime) {
            q.put("ts", new BasicDBObject("$gte", startTime));
        }
        if (0 != endTime) {
            q.put("ts", new BasicDBObject("$lte", endTime));
        }

        dbCollection.deleteMany(q);
    }

    @Override
    public void groupchatMsgDel(String roomJid, int type) {
        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + roomJid);
        BasicDBObject q = new BasicDBObject();
        try {
            if (0 == type) {
                // 删除一个月前的聊天记录
                long onedayNextDay = DateUtil.getOnedayNextDay(DateUtil.currentTimeSeconds(), 30, 1);
                logger.info("上个月的时间：" + onedayNextDay);
                q.put("timeSend", new BasicDBObject("$lte", onedayNextDay));
                dbCollection.deleteMany(q);
            } else if (1 == type) {
                final long num = 100000;
                long count = dbCollection.countDocuments();
                if (count <= num) {
                    throw new ServiceException("数量小于等于" + num);
                }
                // 删除十万条前的聊天记录
                FindIterable<Document> timeSend = dbCollection.find().sort(new BasicDBObject("timeSend", -1)).skip((int) num).limit(1);
                Document document = timeSend.first();
                Long timeSendKey = document.getLong("timeSend");
                Document query = new Document();
                query.put("timeSend", new Document(MongoOperator.LTE, timeSendKey));
                dbCollection.deleteMany(query);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }


    @Override
    public PageResult<Document> roomDetail(int page, int limit, String room_jid) {

        MongoCollection<Document> dbCollection = getImRoomDB().getCollection(MUCMSG + room_jid);
        Document q = new Document();
        q.put("contentType", 1);
        if (!StringUtil.isEmpty(room_jid)) {
            q.put("room_jid", room_jid);
        }
        long total = dbCollection.countDocuments(q);
        logger.info("消息 总条数" + total);

        List<Document> pageData = Lists.newArrayList();
        MongoCursor<Document> cursor = dbCollection.find(q).sort(new Document("_id", 1)).skip((page - 1) * limit).limit(limit).iterator();
        while (cursor.hasNext()) {
            Document dbObj = cursor.next();
            try {
                JSONObject body = JSONObject.parseObject(dbObj.getString("message"));
                dbObj.put("content", body.get("content"));
                dbObj.put("fromUserName", body.get("fromUserName"));
            } catch (Exception e) {
                dbObj.put("content", "--");
                logger.error(e.getMessage());
            }
            pageData.add(dbObj);
        }
        PageResult<Document> result = new PageResult<Document>();
        result.setData(pageData);
        result.setCount(total);
        return result;
    }


    /**
     * 增加消息已读数量
     */
    public Document wrapReadCount(Document next, MongoCollection<Document> collection) {
        // 设置消息已读数量
        Document readCountQuery = new Document();
        readCountQuery.put("roomJid", next.get("room_jid"));
        readCountQuery.put("messageId", next.get("messageId"));
        MongoCursor<Document> iterator = collection.find(readCountQuery).iterator();
        Document document = null;
        while (iterator.hasNext()) {
            document = iterator.next();
            List<Integer> userIds = document.getList("redUsers", Integer.class);
            next.put("readCount", null != userIds ? userIds.size() : KConstants.ZERO);
        }
        return next;
    }


}
