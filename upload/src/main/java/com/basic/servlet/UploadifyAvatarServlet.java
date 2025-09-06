package com.basic.servlet;

import java.io.File;
import java.util.List;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.google.common.base.Joiner;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.vo.JMessage;
import com.basic.commons.vo.UploadItem;

@WebServlet("/upload/UploadifyAvatarServlet")
public class UploadifyAvatarServlet extends BaseServlet {
    private static final long serialVersionUID = 1L;

    public UploadifyAvatarServlet() {
        super();
    }

    @Override
    protected JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart) {
        JMessage jMessage = null;
        long userId = 0;
        FileItem item = null;
        if (null == multipart) return new JMessage(1020101, "表单解析失败");
        for (FileItem fileItem : multipart) {
            if (fileItem.isFormField()) {
                if ("userId".equals(fileItem.getFieldName())) {
                    userId = Long.parseLong(fileItem.getString());
                }
            } else {
                item = fileItem.getSize() > 0 ? fileItem : null;
            }
        }

        if (null == item) {
            jMessage = new JMessage(1010101, "缺少上传文件");
        } else if (0 == userId) {
            jMessage = new JMessage(1010101, "缺少请求参数");
        }

        if (null != jMessage)
            return jMessage;

        File[] uploadPath = ConfigUtils.getAvatarPath(userId);
        String formatName = ConfigUtils.getFormatName(item.getName());
        String fileName = Joiner.on("").join(userId, ".", "jpg");
        File oFile = new File(uploadPath[0], fileName);
        File tFile = new File(uploadPath[1], fileName);
        try {
            if ("png".equals(formatName))
                FileUtils.transferFromPng(item.getInputStream(), oFile, tFile, formatName);
            else
                FileUtils.transfer(item.getInputStream(), oFile, tFile, formatName);
            String oUrl = ConfigUtils.getUrl(oFile);
            String tUrl = ConfigUtils.getUrl(tFile);
            log("UploadifyAvatarServlet uploadEd " + oUrl);
            log("UploadifyAvatarServlet uploadEd " + tUrl);
            jMessage = new JMessage(1, null, new UploadItem(null, oUrl, tUrl));
            return jMessage;
        } catch (Exception e) {
            e.printStackTrace();
            return errorMessage(e);
        } finally {
            closeRecoverFileItem(multipart);
        }

    }

}
