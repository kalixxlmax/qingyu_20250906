package com.basic.commons.utils;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryOperators;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.basic.domain.ResourceFile;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * 文件资源数据库操作工具类
 */
public class ResourcesDBUtils {

    private static final Logger log = LoggerFactory.getLogger(ResourcesDBUtils.class);

    private static MongoCollection resourceCollection;
    private static MongoCollection delFilesCollection;

    private static final String dbName = "resources";                      // 库名
    private static final String resources_collection_name = "resources";   // 资源表名



    private static MongoCollection AVATAR_GROUP_COLLECTION;

    // 资源表名
    private static final String AVATAR_GROUP_NAME = "avatar_group";


    private static final String delFile_collection_name = "del_files";     // 文件删除表名


    public interface Expire {
        static final int DAY1 = 86400;
        static final int DAY7 = 604800;
        static final int HOUR12 = 43200;
        static final int HOUR = 3600;
    }

    static {
        String urIStr = ConfigUtils.getSystemConfig().getDbUri();
        if (StringUtils.isEmpty(urIStr)) {
            log.error("===> error msg dbUri is null =====>");
        }
        MongoClient mongoClient = MongoDBUtil.getMongoClient(urIStr);
        MongoDatabase db = mongoClient.getDatabase(dbName);
        resourceCollection = db.getCollection(dbName);
    }

    private static MongoCollection getResourceCollection() {
        if (null != resourceCollection)
            return resourceCollection;
        try {
            String resourceUri = ConfigUtils.getSystemConfig().getDbUri();
            if (StringUtils.isEmpty(resourceUri)) {
                log.error("===> error msg dbUri is null =====>");
            }
            MongoClient mongoClient = MongoDBUtil.getMongoClient(resourceUri);
            MongoDatabase db = mongoClient.getDatabase(dbName);
            resourceCollection = db.getCollection(resources_collection_name);
            return resourceCollection;
        } catch (Exception e) {
            e.printStackTrace();
            return resourceCollection;
        }

    }


    private static MongoCollection getDelFilesCollection() {
        if (null != delFilesCollection) return delFilesCollection;
        try {
            String delUri = ConfigUtils.getSystemConfig().getDelFileUri();
            if (StringUtils.isEmpty(delUri)) {
                delUri = ConfigUtils.getSystemConfig().getDbUri();
                if (StringUtils.isEmpty(delUri)) {
                    log.error("===> error msg dbUri is null =====>");
                    return null;
                }
            }
            MongoClient mongoClient = MongoDBUtil.getMongoClient(delUri);
            MongoDatabase db = mongoClient.getDatabase(dbName);
            delFilesCollection = db.getCollection(delFile_collection_name);
            return delFilesCollection;
        } catch (Exception e) {
            e.printStackTrace();
            return delFilesCollection;
        }

    }

    /**
     * 群组头像表
     * @return
     */
    private static MongoCollection getAvatarGroupCollection() {
        if (null != AVATAR_GROUP_COLLECTION) return AVATAR_GROUP_COLLECTION;
        try {
            String delUri = ConfigUtils.getSystemConfig().getDbUri();
            if (StringUtils.isEmpty(delUri)) {
                log.error("===> error msg dbUri is null =====>");
            }
            MongoClient mongoClient = MongoDBUtil.getMongoClient(delUri);
            MongoDatabase db = mongoClient.getDatabase(dbName);
            AVATAR_GROUP_COLLECTION = db.getCollection(AVATAR_GROUP_NAME);
            return AVATAR_GROUP_COLLECTION;
        } catch (Exception e) {
            e.printStackTrace();
            return AVATAR_GROUP_COLLECTION;
        }

    }

    //从 del collection 获取要删除的文件url，然后删除对应的文件
    public static void delFileFromDelCollection() {
        MongoCollection delCollection = getDelFilesCollection();
        MongoCursor<Document> iterator = delCollection.find().limit(1000).iterator();
        String fileUri = "";
        String fileChildPath = "";
        if (!iterator.hasNext())
            return;
        while (iterator.hasNext()) {
            Document result = iterator.next();
            try {
                fileUri = "" + result.getString("value");
                if (fileUri.contains(",")) //uri 可能有多个
                    fileUri = fileUri.split(",")[0];

                if (fileUri.startsWith("http://") || fileUri.startsWith("https://")) {
                    String tempPath = fileUri.substring(fileUri.indexOf("//") + 2);
                    fileChildPath = tempPath.substring(tempPath.indexOf("/"));
                }

                if (StringUtils.isEmpty(fileChildPath)) {
                    delCollection.deleteOne(new Document("_id", result.getString("_id")));
                    continue;
                }
                if (1 == ConfigUtils.getSystemConfig().getIsOpenfastDFS() || fileChildPath.startsWith("group"))
                    FastDFSUtils.deleteFile(fileChildPath);
                else {
                    FileUtils.deleteFile(fileChildPath);
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            delCollection.deleteOne(new Document("_id", result.getString("_id")));
            log.info("File " + fileUri + " Already Delete");
        }
        //递归调用
        delFileFromDelCollection();
    }

    public static void saveFileUrl(int type, String url, double validTime) {
        saveFileUrl(type, url, validTime, null, null);
    }

    public static void saveFileUrl(int type, String url, double validTime, String md5, String fileType) {
        saveFileUrl(type, url, null, validTime, md5, fileType);
    }

    /**
     * @param validTime   文件的有效期   0/-1 为永久,否则为有效期多少天
     */
    public static void saveFileUrl(int type, String url, String minUrl, double validTime, String md5, String fileType) {
        if(StrUtil.isEmpty(url)||StrUtil.isEmpty(md5)){
            return;
        }
        long cuTime = System.currentTimeMillis() / 1000;
        long endTime = validTime>0 ? cuTime + (long) (Expire.DAY1 * validTime) : -1;

        // 获取文件的真实地址  不带 域名
        String path = FileUtils.getAbsolutePath(url);
        String fileName = FileUtils.getFileName(path);
        if (1 == type) {
            String basePath=ConfigUtils.getBasePath().endsWith("/")?ConfigUtils.getBasePath():ConfigUtils.getBasePath()+"/";
            path = basePath + path;
        }

        Document document = new Document("createTime", cuTime);
        document.append("endTime", endTime);
        document.append("url", url);
        if (!StringUtils.isEmpty(minUrl)) {
            document.append("minUrl", minUrl);
        }
        document.append("path", ConfigUtils.getFormatUrl(path));
        document.append("type", type);
        document.append("status", 1);
        document.append("fileName", fileName);
        document.append("citations", 1);
        if (!StringUtils.isEmpty(md5)) {
            document.append("md5", md5);
        }
        if (!StringUtils.isEmpty(fileType)) {
            document.append("fileType", fileType);
        }
        getResourceCollection().insertOne(document);
    }


    public static void saveGroupAvatarFile(String jid, boolean falg) {

        Document query = new Document("_id", jid);


        Document update = new Document("$set",new Document("falg", falg));

        getAvatarGroupCollection().updateOne(query,update,new UpdateOptions().upsert(true));
    }

    public static boolean getGroupAvatarIsExists(String jid) {

        Document query = new Document("_id", jid);




        return 0<getAvatarGroupCollection().countDocuments(query);
    }

    /**
     * 根据文件名查找数据
     */
    public static FindIterable<Document> getFileByFileName(String fileName) {
        if (StrUtil.isNotBlank(fileName)){
            BasicDBObject query = new BasicDBObject();
            query.put("fileName", fileName);
            return getResourceCollection().find(query);
        }
        return null;
    }


    /**
     * 根据MD5查询是否继续增加文件
     */
    public static ResourceFile getFileByMD5(String md5) {
        if (StrUtil.isNotBlank(md5)){
            BasicDBObject query = new BasicDBObject();
            query.put("md5", md5);
            return convert(getResourceCollection().find(query));
        }
        return null;
    }

    /**
     * 根据URL查询文件
     */
    public static ResourceFile getFileByUrl(String url) {
        if (StrUtil.isNotBlank(url)) {
            BasicDBObject query = new BasicDBObject();
            BasicDBList values = new BasicDBList();
            values.add(new BasicDBObject("url",url));
            values.add(new BasicDBObject("minUrl", url));
            query.put("$or", values);
            return convert(getResourceCollection().find(query));
        }
        return null;
    }

    /**
     * 根据md5删除记录
     */
    public static void deleteFileByMD5(String md5) {
        if (StrUtil.isNotBlank(md5)){
            log.info("deleteFileByMD5 md5 is {}", md5);
            getResourceCollection().deleteOne(new Document("md5", md5));
        }
    }

    /**
     * @param fileName 文件的fileName
     */
    public static void deleteFileByFileName(String fileName) {
        if (StrUtil.isNotBlank(fileName)){
            getResourceCollection().deleteMany(new Document("fileName", fileName));
        }
    }

    /**
     * 根据文件名修改文件引用次数
     * @param fileName  文件名
     * @param citations 引用次数
     */
    public static UpdateResult incCitationsByFileName(String fileName, int citations) {
        if (StrUtil.isNotBlank(fileName)){
            BasicDBObject query = new BasicDBObject();
            query.put("fileName", fileName);
            return getResourceCollection().updateMany(query, new BasicDBObject("$inc", new BasicDBObject("citations", citations)));
        }
        return null;
    }

    /**
     * 根据文件 MD5 进行引用次数自增
     */
    public static void incCitationsByMd5(String md5, int number) {
        if (StrUtil.isNotBlank(md5)){
            BasicDBObject query = new BasicDBObject();
            query.put("md5", md5);
            getResourceCollection().updateMany(query, new BasicDBObject("$inc", new BasicDBObject("citations", number))).getModifiedCount();
        }
    }


    private static ResourceFile convert(Iterable<Document> iterable){
        for (Document document : iterable) {
            return convert(document);
        }
        return null;
    }

    private static ResourceFile convert(Document document){
        ResourceFile resourceFile = new ResourceFile();
        resourceFile.setUrl(document.getString("url"));
        resourceFile.setMinUrl(document.getString("minUrl"));
        resourceFile.setFileType(document.getString("fileType"));
        resourceFile.setFileName(document.getString("fileName"));
        resourceFile.setCitations(document.getInteger("citations"));
        resourceFile.setMd5(document.getString("md5"));
        resourceFile.setPath(document.getString("path"));
        resourceFile.setType(Byte.valueOf(document.getInteger("type")+""));
        resourceFile.setStatus(Byte.valueOf(document.getInteger("status")+""));
        resourceFile.setCreateTime(document.getLong("createTime"));
        resourceFile.setEndTime(document.getLong("endTime"));
        resourceFile.setNsfwScore(document.getDouble("nsfwScore"));
        log.info("file is exists value is {}", JSONObject.toJSONString(resourceFile));
        return resourceFile;
    }

    public static void runDeleteFileTask() {
        long cuTime = System.currentTimeMillis() / 1000;
        Document query = new Document("endTime", new Document("$gt", 0).append("$lt", cuTime));
        MongoCollection collection = getResourceCollection();
        long count = collection.count(query);
        MongoCursor<Document> cursor = collection.find(query).iterator();
        Document resultDoc;
        String path;
        int type;
        log.info(" runDeleteFileTask query count {} ", count);
        while (cursor.hasNext()) {
            resultDoc = cursor.next();
            log.info(" run delete task {} ", resultDoc);
            path = resultDoc.getString("path");
            type = resultDoc.getInteger("type");
            if (StringUtils.isEmpty(path))
                path = resultDoc.getString("url");
            if (StringUtils.isEmpty(path))
                continue;
            if (2 == type)
                FastDFSUtils.deleteFile(path);
            else {
                FileUtils.deleteFile(path);
            }
        }
        getResourceCollection().deleteMany(query);
    }

    public static void updateImagesNSFW(String md5, Double nsfwScore) {

        // Document query = new Document("url", url);
        Document query = new Document("md5", md5);

        Document update = new Document("$set",new Document("nsfwScore", nsfwScore));

        getResourceCollection().updateOne(query,update,new UpdateOptions().upsert(true));
    }
}

