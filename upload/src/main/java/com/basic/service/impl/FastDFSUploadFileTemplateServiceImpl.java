package com.basic.service.impl;

import com.basic.commons.utils.*;
import com.basic.commons.vo.FastDFSFile;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.UploadFileModel;
import com.basic.commons.vo.UploadItem;
import com.basic.domain.ResourceFile;
import com.basic.factory.UploadFileFactory;
import com.basic.service.AbstractUploadFileTemplateService;
import com.basic.service.UploadFileTemplateService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

@Service
public class FastDFSUploadFileTemplateServiceImpl extends AbstractUploadFileTemplateService implements UploadFileTemplateService, InitializingBean {


    @Override
    public UploadItem doSaveFileStore(UploadFileModel model) {
        try{
           String fileName = StringUtils.isEmpty(model.getFileName()) ? model.getOriginalFilename():model.getFileName();
            String suffixName = FileUtils.getSuffixName(Objects.requireNonNull(model.getFileName()));
            FileType fileType = ConfigUtils.getFileType(suffixName);
            String fileMD5 = FileUtils.getFileMD5(model.getMultipartFile().getInputStream());
            String path = uploadToFastDFS(input2byte(model.getMultipartFile().getInputStream()), model.getFileName(),fileMD5,fileType.getBaseName());
            if (!StringUtils.isEmpty(path)){
                return new UploadItem(fileName, path, path, (byte) 1, null,fileType);
            }
            return null;
        }catch (Exception e){
            return null;
        }
    }

    /*@Override
    public UploadItem doSaveFileStoreInputStream(InputStream inputStream, int id, String fileName, File[] uploadPath, boolean isTranPng) {
        String path = uploadToFastDFS(input2byte(inputStream), fileName,null,"image");
        if (!StringUtils.isEmpty(path)){
            return new UploadItem(fileName, path, path, (byte) 1, null,FileType.Image);
        }
        return null;
    }*/

    protected String uploadToFastDFS(byte[] bytes, String fileName, String md5, String fileType){
        FastDFSFile fastDFSFile=new FastDFSFile();
        fastDFSFile.setName(fileName);
        fastDFSFile.setSize((long) bytes.length);
        fastDFSFile.setContent(bytes);
        String path= FastDFSUtils.uploadFile(fastDFSFile);
        path = ConfigUtils.getFastDFSUrl(path);

        return path;
    }

    public  final byte[] input2byte(InputStream inStream){
        try {
            ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
            byte[] buff = new byte[1024];
            int rc = 0;
            while ((rc = inStream.read(buff, 0, 1024)) > 0) {
                swapStream.write(buff, 0, rc);
            }
            byte[] in2b = swapStream.toByteArray();
            return in2b;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 上传文件到 FastDFS
     */
    protected String uploadToFastDFS(MultipartFile file, double validTime, String md5, String fileType) throws IOException {
        FastDFSFile fastDFSFile=new FastDFSFile();
        String fileName=file.getOriginalFilename();
        fastDFSFile.setName(fileName);
        fastDFSFile.setSize(file.getSize());
        fastDFSFile.setContent(file.getBytes());
        String path= FastDFSUtils.uploadFile(fastDFSFile);
        path = ConfigUtils.getFastDFSUrl(path);
        // 保存文件 URL 地址到数据库
        ResourcesDBUtils.saveFileUrl(2,path,path,validTime,md5,fileType);
        return path;
    }

    @Override
    public boolean deleteFile(ResourceFile resourceFile) {
        ResourcesDBUtils.deleteFileByMD5(resourceFile.getMd5());
        String childPath = FileUtils.getAbsolutePath(resourceFile.getUrl());
        return FastDFSUtils.deleteFile(childPath);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        UploadFileFactory.register(UploadFileFactory.FAST_DFS_TYPE, this);
    }
}
