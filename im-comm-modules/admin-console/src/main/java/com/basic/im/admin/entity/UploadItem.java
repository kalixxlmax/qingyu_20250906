package com.basic.im.admin.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @ClassName Resources
 * @Author xie yuan yang
 * @date 2020.10.20 12:11
 * @Description
 */
@Data
@Document(value="resources")
public class UploadItem {
    @Id
    private ObjectId id;
    private String url;
    private String path;
    //1 本机文件系统  2 fastDfs
    private Byte type;
    private Byte status;
    private String fileName;
    private String fileType;
    //引用次数
    private String citations;
    private long createTime;
    private long endTime;
}
