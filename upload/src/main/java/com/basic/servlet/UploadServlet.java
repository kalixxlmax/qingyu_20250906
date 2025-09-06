package com.basic.servlet;

import java.io.*;
import java.util.List;
import java.util.UUID;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.basic.commons.utils.UploadItemUtils;
import com.basic.domain.ResourceFile;
import org.apache.commons.fileupload.FileItem;
import org.springframework.util.StringUtils;

import com.google.common.base.Joiner;
import com.basic.commons.Amr2mp3;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.JMessage;
import com.basic.commons.vo.UploadItem;

@WebServlet("/upload/UploadServlet")
public class UploadServlet extends BaseServlet {
    private static final long serialVersionUID = 1L;


    @Override
    protected JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart) {
        long start = System.currentTimeMillis();
        JMessage jMessage = null;
        int totalCount = 0;
        // int uploadFlag = 0;
        long userId = 0;
        double validTime = 0;
        try {
            for (FileItem item : multipart) {
                if (item.isFormField()) {
                    if ("validTime".equals(item.getFieldName())) {
                        try { validTime = Double.parseDouble(item.getString()); } catch (NumberFormatException e) { validTime = -1; }
                    }

                    if ("userId".equals(item.getFieldName())) {
                        userId = Long.parseLong(item.getString());
                    }

                } else {
                    if (item.getSize() > 0) { totalCount++; }
                }
            }
        } catch (Exception e) {
            log(e.getMessage(), e);
            e.printStackTrace();
        }

        if (null == multipart) {
            jMessage = new JMessage(1020101, "表单解析失败");
        } else if (0 == totalCount) {
            jMessage = new JMessage(1010101, "缺少上传文件");
        }
        if (null != jMessage){ return jMessage; }
        try {
            if (1 == getSystemConfig().getIsOpenfastDFS()) {
                jMessage = fastDFSHander(multipart, validTime);
            } else{
                jMessage = defHander(multipart, userId, validTime);
            }
            int successCount = jMessage.getIntValue("success");
            jMessage.put("total", totalCount);
            jMessage.put("failure", totalCount - successCount);
            jMessage.put("time", System.currentTimeMillis() - start);
            log("upload time " + (System.currentTimeMillis() - start));
        } catch (Exception e) {
            log(e.getMessage(), e);

        } finally {
            closeRecoverFileItem(multipart);
        }
        return jMessage;
    }

    protected JMessage fastDFSHander(List<FileItem> multipart, double validTime) {
        JMessage message = null;
        int successCount = 0;
        UploadItemUtils uploadItemUtils=UploadItemUtils.getUploadUtils();
        for (FileItem item : multipart) {
            // 进行 MD5 校验
            String fileMD5 = null;
            try{
                fileMD5 = FileUtils.getFileMD5(item.getInputStream());
                ResourceFile resourceFile = ResourcesDBUtils.getFileByMD5(fileMD5);
                if (resourceFile!=null){
                    successCount++;
                    uploadItemUtils.addImage(new UploadItem(item.getName(), resourceFile.getUrl(), resourceFile.getMinUrl(), (byte) 1, null));
                    ResourcesDBUtils.incCitationsByMd5(resourceFile.getMd5(),1);
                    continue;
                }
            }catch (Exception ignored){}

            if (item.isFormField() || item.getSize() < 1){ continue; }
            String fileName = item.getName();
            String formatName = getFormatName(fileName);
            FileType fileType = getFileType(formatName);
            String path = uploadToFastDFS(item, validTime,fileMD5,fileType.getBaseName());
            if (StringUtils.isEmpty(path)){ continue; }
            successCount++;
            if (FileType.Image == fileType) {// 图片
                uploadItemUtils.addImage(new UploadItem(item.getName(), path, path, (byte) 1, null));
            } else {//其他
                uploadItemUtils.addEntityToFileType(fileType,new UploadItem(fileName, path, path, (byte) 1, null));
            }
        }
        message = new JMessage(1, null, uploadItemUtils);
        message.put("success", successCount);
        return message;
    }

    protected JMessage defHander(List<FileItem> multipart, long userId, double validTime) {
        JMessage jMessage;
        int successCount = 0;
        String oUrl,tUrl;
        UploadItemUtils uploadItemUtils = UploadItemUtils.getUploadUtils();

        for (FileItem item : multipart) {
            UploadItem uploadItem;
            if (item.isFormField() || item.getSize() < 1) { continue; }
            String oFileName = item.getName();
            String formatName = FileUtils.getSuffixName(oFileName);
            String fileName = Joiner.on("").join(UUID.randomUUID().toString().replace("-", ""),
                    (StringUtils.isEmpty(formatName) ? "": "."), formatName);
            FileType fileType = ConfigUtils.getFileType(formatName);
            File[] uploadPath = ConfigUtils.getUploadPath(userId, fileType);
            if (FileType.Image == fileType) {//图片
                try {
                    // 检测文件MD5
                    String fileMD5 = FileUtils.getFileMD5(item.getInputStream());
                    ResourceFile resourceFile = ResourcesDBUtils.getFileByMD5(fileMD5);
                    if (resourceFile!=null){
                        successCount++;
                        uploadItemUtils.addImage(new UploadItem(item.getName(), resourceFile.getUrl(), resourceFile.getMinUrl(), (byte) 1, null));
                        ResourcesDBUtils.incCitationsByMd5(fileMD5,1);
                        continue;
                    }
                    // 文件不存在，保存文件
                    File oFile = new File(uploadPath[0], fileName);
                    File tFile = new File(uploadPath[1], fileName);
                    FileUtils.transfer(item.getInputStream(), oFile, tFile, formatName);
                    successCount++;
                    oUrl = getUrl(oFile);
                    tUrl = getUrl(tFile);
                    ResourcesDBUtils.saveFileUrl(1, oUrl,tUrl, validTime,fileMD5,FileType.Image.getBaseName());
                    uploadItem = new UploadItem(item.getName(), oUrl, tUrl, (byte) 1, null);
                } catch (Exception e) {
                    uploadItem = new UploadItem(item.getName(), null, (byte) 0, e.getMessage());
                }
                uploadItemUtils.addImage(uploadItem);
            } else { //其他
                try {
                    // 校验文件MD5
                    String oFileMD5 = FileUtils.getFileMD5(item.getInputStream());
                    ResourceFile resourceFile = ResourcesDBUtils.getFileByMD5(oFileMD5);
                    if (resourceFile!=null){
                        successCount++;
                        uploadItemUtils.addEntityToFileType(resourceFile.getFileType(),new UploadItem(item.getName(), resourceFile.getUrl(), (byte) 1, null));
                        ResourcesDBUtils.incCitationsByMd5(oFileMD5,1);
                        continue;
                    }
                    File oFile = new File(uploadPath[0], fileName);
                    FileUtils.transfer(item.getInputStream(), oFile);
                    if ((fileName.contains(".amr")) && 1 == getSystemConfig().getAmr2mp3()) {
                        File __source = new File(oFile.getPath());
                        File __target = new File(oFile.getPath().replaceAll(".amr", ".mp3"));
                        Amr2mp3.changeToMp3(__source.getPath(), __target.getPath());
                        successCount++;
                        oUrl = getUrl(__target);
                        uploadItem = new UploadItem(item.getName(), oUrl, (byte) 1, null);
                    } else {
                        successCount++;
                        oUrl = getUrl(oFile);
                        uploadItem = new UploadItem(item.getName(), oUrl, (byte) 1, null);
                    }
                    ResourcesDBUtils.saveFileUrl(1, oUrl, -1,FileUtils.getFileMD5(oFile),fileType==null?"":fileType.getBaseName());
                } catch (Exception e) {
                    uploadItem = new UploadItem(item.getName(), null, (byte) 0, e.getMessage());
                }
                uploadItemUtils.addEntityToFileType(fileType, uploadItem);
            }
        }
        jMessage = new JMessage(1, null, uploadItemUtils);
        System.err.println(jMessage.toJSONString());
        jMessage.put("success", successCount);
        return jMessage;
    }
}
