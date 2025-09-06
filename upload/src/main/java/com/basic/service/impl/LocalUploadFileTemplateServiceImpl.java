package com.basic.service.impl;

import cn.hutool.http.HttpUtil;
import com.basic.commons.vo.UploadFileModel;
import com.basic.service.AbstractUploadFileTemplateService;
import com.basic.service.CheckImagesService;
import com.google.common.base.Joiner;
import com.basic.commons.Amr2mp3;
import com.basic.commons.utils.*;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.UploadItem;
import com.basic.domain.ResourceFile;
import com.basic.factory.UploadFileFactory;
import com.basic.service.UploadFileTemplateService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class LocalUploadFileTemplateServiceImpl extends AbstractUploadFileTemplateService implements UploadFileTemplateService, InitializingBean {


    @Autowired(required = false)
    private CheckImagesService checkImagesService;

    String imagePreCheck(UploadFileModel model, String fileName, String contentType,File[] uploadPath) {

        MultipartFile multipartFile = model.getMultipartFile();
        // 是否开启色情图片过滤
        Integer openFilter = ConfigUtils.getSystemConfig().getOpenNsfwFilter();

        // 不开启 色情图片过滤
        if (openFilter.equals(0)) {
            return null;
        }

        // 要过滤图片的阈值,0-1之间,官方建议0.85以上算色情图片
        Double nsfwImagesScore = ConfigUtils.getSystemConfig().getNsfwImagesScore();

        // Python的图片鉴黄模块
        final String nsfwModuleUrl = ConfigUtils.getSystemConfig().getNsfwModuleUrl();

        String img = null;
        try {

            // 不能计算MD5值,否则图片的InputStream会丢失

            // 使用putObject上传一个文件到存储桶中
            // model.getInputStream() 图片存了是空白的,
            // img=  uploadThirdStorage(multipartFile.getInputStream(), fileName, contentType);

            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {

                String projectPath = System.getProperty("user.dir");
                String forbid = projectPath + "/src/main/resources/forbid.png";
                File forbidFile = new File(forbid);

                try {

                    HashMap<String, Object> paramMap = new HashMap<>();
                    paramMap.put("file", com.basic.commons.utils.FileUtils.multipartFileToFile(multipartFile));

                    // 第一 ,异步鉴别涩图
                    String result = HttpUtil.post(nsfwModuleUrl, paramMap);
                    Double score = Double.parseDouble(result);

                    String fileMD5 = FileUtils.getFileMD5(multipartFile.getInputStream());

                    if (score > nsfwImagesScore) {
                        log.info("上传的图片涉黄,评分为:" + score);
                        InputStream input = null;
                        input = new FileInputStream(forbidFile);

                        // 第二,异步 违规图片替换
                        // uploadThirdStorage(input, fileName, contentType);
                        File oFile = new File(uploadPath[0], fileName);
                        FileUtils.transfer(input, oFile);

                        // 第三,记录评分
                        // 保存分值

                        // TODO 用MD5,或者文件名来 查询;

                        // ResourcesDBUtils.updateImagesNSFW(imageURL+fileName, score);
                        ResourcesDBUtils.updateImagesNSFW(fileMD5, score);

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                return null;
            });


        } catch (Exception e) {
            e.printStackTrace();
        }


        return img;
    }


    @Override
    protected UploadItem doSaveFileStore(UploadFileModel model) {
        String suffixName = FileUtils.getSuffixName(Objects.requireNonNull(model.getOriginalFilename()));
        String fileName = StringUtils.isEmpty(model.getFileName()) ? Joiner.on("").join(UUID.randomUUID().toString().replace("-", ""), ".", suffixName) : model.getFileName();
        FileType fileType = ConfigUtils.getFileType(suffixName);

        File[] uploadPath = model.getUploadPath();
        if (uploadPath == null) {
            uploadPath = ConfigUtils.getUploadPath(model.getUserId(), fileType);
        }

        UploadItem uploadItem=upload(model,fileName,suffixName,fileType,uploadPath);

        // 图片检测
        imagePreCheck(model,fileName,fileType.getBaseName(),uploadPath);

        return uploadItem;
    }

    public UploadItem upload(UploadFileModel model,String fileName,String suffixName,FileType fileType,File[] uploadPath) {

        String oUrl = null;
        String tUrl = null;
        UploadItem uploadItem=  new UploadItem();
        try {
            File oFile = new File(uploadPath[0], fileName);
            if (FileType.Image == fileType && uploadPath.length == 2) { //图片
                File tFile = new File(uploadPath[1], fileName);
                FileUtils.transfer(model.getMultipartFile().getInputStream(), oFile, tFile, suffixName, model.isTranPng());
                oUrl = ConfigUtils.getUrl(oFile);
                tUrl = ConfigUtils.getUrl(tFile);
                ResourcesDBUtils.saveFileUrl(1, oUrl, tUrl, model.getValidTime(), FileUtils.getFileMD5(model.getMultipartFile().getInputStream()), FileType.Image.getBaseName());
                uploadItem=new UploadItem(model.getOriginalFilename(), oUrl, tUrl, (byte) 1, null, fileType);
            } else if (FileType.Image == fileType && uploadPath.length == 1) {
                FileUtils.transfer(model.getMultipartFile().getInputStream(), oFile);
                oUrl = ConfigUtils.getUrl(oFile);
                ResourcesDBUtils.saveFileUrl(1, oUrl, 0, FileUtils.getFileMD5(oFile), fileType.getBaseName());
                uploadItem= new UploadItem(model.getOriginalFilename(), oUrl, null, (byte) 1, null, fileType);
            } else {
                FileUtils.transfer(model.getMultipartFile().getInputStream(), oFile);
                if ((fileName.contains(".amr")) && 1 == ConfigUtils.getSystemConfig().getAmr2mp3()) {
                    File source = new File(oFile.getPath());
                    File target = new File(oFile.getPath().replaceAll(".amr", ".mp3"));
                    Amr2mp3.changeToMp3(source.getPath(), target.getPath());
                    oUrl = ConfigUtils.getUrl(target);
                } else {
                    oUrl = ConfigUtils.getUrl(oFile);
                }
                uploadItem = new UploadItem(model.getOriginalFilename(), oUrl, (byte) 1, null);
                ResourcesDBUtils.saveFileUrl(1, oUrl, -1, FileUtils.getFileMD5(oFile), fileType == null ? "" : fileType.getBaseName());
                uploadItem.setFileType(fileType);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uploadItem;
    }

    /*@Override
    public UploadItem doSaveFileStoreInputStream(InputStream inputStream, int id, String fileName, File[] uploadPath, boolean isTranPng) {
        String suffixName = FileUtils.getSuffixName(Objects.requireNonNull(fileName));
        fileName = StringUtils.isEmpty(fileName)?Joiner.on("").join(UUID.randomUUID().toString().replace("-", ""), ".",suffixName):fileName;
        FileType fileType = ConfigUtils.getFileType(suffixName);
        if (uploadPath == null) {
            uploadPath = ConfigUtils.getUploadPath(id, fileType);
        }
        String oUrl,tUrl;

        File oFile = new File(uploadPath[0], fileName);
        File tFile = new File(uploadPath[1], fileName);
        try {
            FileUtils.transfer(inputStream, oFile, tFile, suffixName,isTranPng);
            oUrl = ConfigUtils.getUrl(oFile);
            tUrl = ConfigUtils.getUrl(tFile);
            return new UploadItem(fileName, oUrl, tUrl, (byte) 1, null,fileType);
        } catch (Exception process) {
            process.printStackTrace();
        }




        return null;

    }*/

    @Override
    public boolean deleteFile(ResourceFile resourceFile) {
        ResourcesDBUtils.deleteFileByMD5(resourceFile.getMd5());
        return FileUtils.deleteFile(resourceFile.getPath());
    }

    @Override
    public void afterPropertiesSet() {
        UploadFileFactory.register(UploadFileFactory.LOCAL_TYPE, this);
    }
}
