package com.basic.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.oss.common.utils.BinaryUtil;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.*;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.vo.UploadFileModel;
import com.basic.controller.UploadController;
import com.basic.factory.UploadFileFactory;
import com.basic.service.AbstractThirdUploadFileTemplateService;
import com.basic.service.ThirdUploadFileTemplateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.internal.OSSHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.util.Formatter;

@Service
public class OSSUploadFileTemplateServiceImpl extends AbstractThirdUploadFileTemplateService implements ThirdUploadFileTemplateService, InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(OSSUploadFileTemplateServiceImpl.class);


    /**
     * OSS客户端
     */
    private volatile OSS ossClient;

    public OSS getOssClient() {
        if(null==ossClient){
            createClient();
        }
        return ossClient;
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

    /**
     * 上传文件到 oss
     */
    @Override
    public String uploadThirdStorage(InputStream file, String fileName, String contentType) {

        PutObjectResult putObjectResult=null;
        // 2、上传文件到 OSS
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader(OSSHeaders.OSS_STORAGE_CLASS, StorageClass.Standard.toString());
            if (StrUtil.isNotBlank(contentType)){
                metadata.setContentType(contentType);
            }
            metadata.setObjectAcl(CannedAccessControlList.PublicRead);
            metadata.setContentLength(Integer.valueOf(file.available()).longValue());
            PutObjectRequest putObjectRequest = new PutObjectRequest(ConfigUtils.getSystemConfig().getOssBucket(), fileName, file);


            putObjectRequest.setMetadata(metadata);
            putObjectResult = getOssClient().putObject(putObjectRequest);

            return ConfigUtils.getSystemConfig().getOssImageDomain() + fileName;
        } catch (IOException e) {
            log.error(e.getMessage(),e);
        }finally {
            if(null!=putObjectResult&&null!=putObjectResult.getResponse()){
                try {
                    putObjectResult.getResponse().close();
                } catch (IOException e) {
                    log.error(e.getMessage(),e);
                }
            }
        }
        return "";
    }

    final String THUMBNAIL_STYLE_TYPE = "image/resize,w_%s,h_%s";

    @Override
    public String createThumbnailImage(String sourceImage,String targetImage){
        String bucketName = ConfigUtils.getSystemConfig().getOssBucket();
        try {
            // 将图片缩放为固定宽高100 px，并命名为example-resize.png后转存到当前Bucket。
            StringBuilder sbStyle = new StringBuilder();
            Formatter styleFormatter = new Formatter(sbStyle);


            String styleType=String.format(THUMBNAIL_STYLE_TYPE,
                    ConfigUtils.getSystemConfig().getThumbnailSize(),
                    ConfigUtils.getSystemConfig().getThumbnailSize());

            //String styleType="image/resize,w_100,h_100";

            //log.info(styleType);

            styleFormatter.format("%s|sys/saveas,o_%s,b_%s", styleType,
                    BinaryUtil.toBase64String(targetImage.getBytes()),
                    BinaryUtil.toBase64String(bucketName.getBytes()));


            ProcessObjectRequest request = new ProcessObjectRequest(bucketName, sourceImage, sbStyle.toString());
            GenericResult processResult = ossClient.processObject(request);
            String json = IOUtils.readStreamAsString(processResult.getResponse().getContent(), "UTF-8");
            if(null!=json) {
                JSONObject jsonObject = JSON.parseObject(json);
                    return ConfigUtils.getSystemConfig().getOssImageDomain() +jsonObject.getString("object");
            }
            processResult.getResponse().getContent().close();
            return json;


        } catch (Exception e) {
           log.error(e.getMessage());
           try {
               ossClient.copyObject(bucketName,sourceImage,bucketName,targetImage);
               return ConfigUtils.getSystemConfig().getOssImageDomain()+targetImage;
           }catch (Exception ex){
               log.error(ex.getMessage(),ex);
               return null;
           }


        }
    }

    @Override
    public boolean deleteThirdFile(String url) {
        try {
            getOssClient().deleteObject(ConfigUtils.getSystemConfig().getOssBucket(), url);
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }

        return true;
    }


    /**
     * 创建 OSS 客户端
     */
    @Override
    public void createClient() {
        if (ossClient == null) {
            ossClient = new OSSClientBuilder().build(ConfigUtils.getSystemConfig().getOssEndpoint(),
                    ConfigUtils.getSystemConfig().getOssAccessKey(),
                    ConfigUtils.getSystemConfig().getOssSecretKey());
        }
    }

    /**
     * 关闭 OSS 客户端
     */
    @Override
    public void closeClient() {
        /*if (ossClient != null) {
            ossClient.shutdown();
            ossClient=null;
        }*/
    }


    /**
     * 注册文件上传服务
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        UploadFileFactory.register(UploadFileFactory.OSS_TYPE, this);
    }
}
