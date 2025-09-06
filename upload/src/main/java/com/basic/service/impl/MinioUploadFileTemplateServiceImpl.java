package com.basic.service.impl;

import cn.hutool.http.HttpUtil;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.UploadItemUtils;
import com.basic.commons.vo.UploadFileModel;
import com.basic.commons.vo.UploadItem;
import com.basic.factory.UploadFileFactory;
import com.basic.service.AbstractThirdUploadFileTemplateService;
import com.basic.service.ThirdUploadFileTemplateService;
import io.minio.MinioClient;
import io.minio.errors.*;
import io.minio.policy.PolicyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * @Description:
 * @Author weiXiangMing
 * @Date 2021/11/11 15:26
 */
@Service
public class MinioUploadFileTemplateServiceImpl extends AbstractThirdUploadFileTemplateService implements ThirdUploadFileTemplateService, InitializingBean {


    private static final Logger log = LoggerFactory.getLogger(MinioUploadFileTemplateServiceImpl.class);


    /**
     * MinioClient客户端
     */
    private volatile MinioClient minioClient;

    public MinioClient getMinioClient() {
        if (null == minioClient) {
            createClient();
        }
        return minioClient;
    }

    @Override
    public String uploadThirdStorage(UploadFileModel model, String fileName, String contentType) {
        try {
            return uploadThirdStorage(model.getMultipartFile().getInputStream(), fileName,  contentType);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String uploadThirdStorage(InputStream inputStream, String fileName, String contentType) {

        try {

            boolean isExist = getMinioClient().bucketExists(ConfigUtils.getSystemConfig().getMinioBucketName());
            if (isExist) {
                // 存储桶已经存在
            } else {
                //创建存储桶并设置只读权限
                getMinioClient().makeBucket(ConfigUtils.getSystemConfig().getMinioBucketName());
                getMinioClient().setBucketPolicy(ConfigUtils.getSystemConfig().getMinioBucketName(), "*.*", PolicyType.READ_ONLY);
            }

            getMinioClient().putObject(ConfigUtils.getSystemConfig().getMinioBucketName(), fileName, inputStream, contentType);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return ConfigUtils.getSystemConfig().getMinioImageDomain() + fileName;
    }

    @Override
    public String createThumbnailImage(String sourceImage, String targetImage) {
        // 阿里云的oss才需要这个方法,但是旧方法已经写乱了,只能继承了;
        return null;
    }

    @Override
    public boolean deleteThirdFile(String url) {

        try {
            getMinioClient().removeObject(ConfigUtils.getSystemConfig().getMinioBucketName(), url);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    @Override
    public void createClient() {

        if (minioClient == null) {
            try {
                minioClient = new MinioClient(ConfigUtils.getSystemConfig().getMinioEndpoint(),
                        ConfigUtils.getSystemConfig().getMinioAccessKey(),
                        ConfigUtils.getSystemConfig().getMinioSecretKey());
            } catch (InvalidEndpointException e) {
                e.printStackTrace();
            } catch (InvalidPortException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void closeClient() {
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        UploadFileFactory.register(UploadFileFactory.MINIO_TYPE, this);
    }

}
