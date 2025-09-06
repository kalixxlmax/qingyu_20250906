package com.basic.factory;


import com.basic.commons.utils.ConfigUtils;
import com.basic.service.UploadFileTemplateService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 文件存储实例工厂
 */
public class UploadFileFactory {

    public static final String LOCAL_TYPE="local";
    public static final String FAST_DFS_TYPE="fastDfs";
    public static final String OSS_TYPE="oss";
    public static final String COS_TYPE="cos";
    public static final String MINIO_TYPE="minio";

    private static final Map<String, UploadFileTemplateService> uploadFileServiceMap = new ConcurrentHashMap<>();

    /**
     * 获取工厂UploadFileTemplateService
     */
    public static UploadFileTemplateService getUploadFileService() {
        return getUploadFileService(ConfigUtils.getSystemConfig().getUploadTypeName());
    }

    public static UploadFileTemplateService getUploadFileService(String type){
        return uploadFileServiceMap.get(type);
    }

    /**
     * 工厂注册
     */
    public static void register(String storyType,UploadFileTemplateService uploadFileTemplateService) {
        uploadFileServiceMap.put(storyType, uploadFileTemplateService);
    }

}
