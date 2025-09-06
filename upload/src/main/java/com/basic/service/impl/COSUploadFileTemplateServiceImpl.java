package com.basic.service.impl;

import cn.hutool.core.util.StrUtil;
import com.basic.commons.vo.UploadFileModel;
import com.basic.service.AbstractThirdUploadFileTemplateService;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.CannedAccessControlList;
import com.qcloud.cos.model.ObjectMetadata;
import com.basic.commons.utils.ConfigUtils;
import com.basic.factory.UploadFileFactory;
import com.basic.service.ThirdUploadFileTemplateService;
import net.coobird.thumbnailator.Thumbnails;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.region.Region;

import javax.imageio.ImageIO;

@Service
public class COSUploadFileTemplateServiceImpl extends AbstractThirdUploadFileTemplateService implements ThirdUploadFileTemplateService, InitializingBean {

    private COSClient cosClient;

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
    public String uploadThirdStorage(InputStream file, String fileName, String contentType) {
        try {
            fileName=formatUrl(fileName);
            ObjectMetadata metadata = new ObjectMetadata();
            if (StrUtil.isNotBlank(contentType)){
                metadata.setContentType(contentType);
            }
            metadata.setContentLength(Integer.valueOf(file.available()).longValue());
            PutObjectRequest putObjectRequest = new PutObjectRequest(ConfigUtils.getSystemConfig().getCosBucket(), fileName, file, metadata);
            putObjectRequest.setCannedAcl(CannedAccessControlList.PublicRead);
            cosClient.putObject(putObjectRequest);


            return ConfigUtils.getSystemConfig().getCosImageDomain() + fileName;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }



    final String THUMBNAIL_STYLE_TYPE = "?imageMogr2/crop/%sx%s/gravity/center";

    @Override
    public String createThumbnailImage(String sourceImage,String targetImage) {
        return sourceImage+String.format(THUMBNAIL_STYLE_TYPE,
                ConfigUtils.getSystemConfig().getThumbnailSize(),
                ConfigUtils.getSystemConfig().getThumbnailSize());
    }





    @Override
    public void createClient() {
        if (cosClient == null) {
            COSCredentials cred = new BasicCOSCredentials(ConfigUtils.getSystemConfig().getCosAccessKey(), ConfigUtils.getSystemConfig().getCosSecretKey());
            Region region = new Region(ConfigUtils.getSystemConfig().getCosRegion());
            ClientConfig clientConfig = new ClientConfig(region);
            cosClient = new COSClient(cred, clientConfig);
        }
    }

    @Override
    public void closeClient() {
       /* if (cosClient != null) {
            cosClient.shutdown();
        }*/
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        UploadFileFactory.register(UploadFileFactory.COS_TYPE, this);
    }


    @Override
    public boolean deleteThirdFile(String url) {
        cosClient.deleteObject(ConfigUtils.getSystemConfig().getCosBucket(),url);
        return true;
    }
}
