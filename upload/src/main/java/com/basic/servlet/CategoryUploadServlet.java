package com.basic.servlet;

import com.google.common.collect.Lists;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.JMessage;
import com.basic.commons.vo.UploadItem;
import org.apache.commons.fileupload.FileItem;
import org.springframework.util.StringUtils;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@WebServlet("/upload/categoryServlet")
public class CategoryUploadServlet extends BaseServlet {
    private static final long serialVersionUID = 1L;


    @Override
    protected JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart) {
        long start = System.currentTimeMillis();
        JMessage jMessage = null;
        int totalCount = 0;
        String categoryId = "";
        FileItem fileItem = null;
        try {
            for (FileItem item : multipart) {
                if (item.isFormField()) {
                    if ("categoryId".equals(item.getFieldName())) {
                        categoryId = item.getString();
                    }
                } else {
                    if (item.getSize() > 0) {
                        fileItem = item;
                        totalCount = 1;
                    }
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
        if (null != jMessage)
            return jMessage;
        try {
            jMessage = defHandler(fileItem, categoryId);
            if (null == jMessage) {
                jMessage = new JMessage();
                jMessage.put("failure", 1);
                return jMessage;
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

    protected JMessage fastDFSHander(List<FileItem> multipart) {
        JMessage message;
        int successCount = 0;

        List<UploadItem> images = Lists.newArrayList();
        List<UploadItem> audios = Lists.newArrayList();
        List<UploadItem> videos = Lists.newArrayList();
        List<UploadItem> others = Lists.newArrayList();
        String oUrl;
        String tUrl;


        for (FileItem item : multipart) {
            UploadItem uploadItem;

            if (item.isFormField() || item.getSize() < 1)
                continue;
            String fileName = item.getName();
            String formatName = getFormatName(fileName);

            FileType fileType = getFileType(formatName);
            String path = uploadToFastDFS(item, 0);
            if (StringUtils.isEmpty(path)) continue;
            oUrl = path;
            tUrl = path;
            successCount++;
            if (FileType.Image == fileType) {// 图片
                try {
                    uploadItem = new UploadItem(item.getName(), oUrl, tUrl, (byte) 1, null);
                } catch (Exception e) {
                    e.printStackTrace();
                    uploadItem = new UploadItem(item.getName(), null, (byte) 0, e.getMessage());
                }
                images.add(uploadItem);
            } else {// 其他
                try {
                    uploadItem = new UploadItem(item.getName(), oUrl, tUrl, (byte) 1, null);
                    if ((fileName.contains(".amr")) && 1 == getSystemConfig().getAmr2mp3()) {
								/*File __source = new File(oFile.getPath());
								File __target = new File(oFile.getPath().replaceAll(".amr", ".mp3"));
								Amr2mp3.changeToMp3(__source.getPath(), __target.getPath());
								successCount++;
								oUrl= SystemConfig.getUrl(__target);
								uploadItem = new UploadItem(__target.getName(),oUrl, (byte) 1, null);*/
                    } else {
								/*successCount++;
								oUrl= SystemConfig.getUrl(oFile);
								uploadItem = new UploadItem(oFileName, oUrl, (byte) 1, null);*/
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    uploadItem = new UploadItem(item.getName(), null, (byte) 0, e.getMessage());
                }
                if (FileType.Audio == fileType)
                    audios.add(uploadItem);
                else if (FileType.Video == fileType)
                    videos.add(uploadItem);
                else
                    others.add(uploadItem);
            }
        }

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("images", images);
        data.put("audios", audios);
        data.put("videos", videos);
        data.put("others", others);


        message = new JMessage(1, null, data);

        message.put("success", successCount);

        return message;
    }

    protected JMessage defHandler(FileItem item, String categoryId) {
        JMessage jMessage;
        int successCount = 0;
        String oUrl;
        String tUrl = null;
        List<UploadItem> images = Lists.newArrayList();
        UploadItem uploadItem;
        String oFileName = item.getName();
        String formatName = ConfigUtils.getFormatName(oFileName);
        String fileName = categoryId + ".jpg";
        FileType fileType = getFileType(formatName);
        File[] uploadPath = ConfigUtils.getCategoryPath();
        if (FileType.Image == fileType) {//图片
            File oFile = new File(uploadPath[0], fileName);
            try {
                FileUtils.transfer(item.getInputStream(), oFile);
                successCount++;
                oUrl = getUrl(oFile);
                ResourcesDBUtils.saveFileUrl(1, oUrl, 0, FileUtils.getFileMD5(oFile), fileType.getBaseName());
                log("categoryServlet uploadEd " + oUrl);
                uploadItem = new UploadItem(item.getName(), oUrl, tUrl, (byte) 1, null);
            } catch (Exception e) {
                e.printStackTrace();
                uploadItem = new UploadItem(item.getName(), null, (byte) 0, e.getMessage());
            }
            images.add(uploadItem);
        }
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("images", images);
        jMessage = new JMessage(1, null, data);
        jMessage.put("success", successCount);
        return jMessage;
    }


}
