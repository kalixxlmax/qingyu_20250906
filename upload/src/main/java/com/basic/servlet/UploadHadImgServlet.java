package com.basic.servlet;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.google.common.base.Joiner;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.vo.JMessage;
import com.basic.commons.vo.UploadItem;


@WebServlet("/upload/UploadAvatarServletByIds")
public class UploadHadImgServlet extends BaseServlet {
    private static final long serialVersionUID = 1L;

    public UploadHadImgServlet() {
        super();
    }

    @Override
    protected JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart) {
        List<UploadItem> data = new ArrayList<UploadItem>();
        List<Long> userIds = new ArrayList<Long>();
        response.addHeader("Access-Control-Allow-Origin", "*");

        JMessage jMessage = null;
        FileItem uploadItem = null;
        long id = 0;
        int version = 0;
        try {
            for (FileItem item : multipart) {
                if ("userId".equals(item.getFieldName()))
                    id = Long.parseLong(item.getString());
                else if ("version".equals(item.getFieldName())) {
                    version = Integer.parseInt(item.getString());
                } else if ("image".equals(item.getFieldName())) {
                    uploadItem = item;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (null == multipart)
            jMessage = new JMessage(1020101, "表单解析失败");
        else if (null == uploadItem)
            jMessage = new JMessage(1010101, "缺少上传文件");
        else if (0 == id || 0 == version)
            jMessage = new JMessage(1010101, "缺少请求参数");
        if (null != jMessage)
            return jMessage;
        //参数不正确直接返回
        userIds.add(id);
        if (1 == version)
            userIds.add(id + 1);
        else userIds.add(id - 1);
        try {
            for (long userId : userIds) {
                File[] uploadPath = ConfigUtils.getAvatarPath(userId);
                String formatName = ConfigUtils.getFormatName(uploadItem.getName());
                String fileName = Joiner.on("").join(userId, ".", "jpg");
                File oFile = new File(uploadPath[0], fileName);
                File tFile = new File(uploadPath[1], fileName);
                FileUtils.transfer(uploadItem.getInputStream(), oFile, tFile, formatName);
                String oUrl = ConfigUtils.getUrl(oFile);
                String tUrl = ConfigUtils.getUrl(tFile);

                log("UploadHadImgServlet uploadEd " + oUrl);
                log("UploadHadImgServlet uploadEd " + tUrl);
                data.add(new UploadItem(null, oUrl, tUrl));
            }

            jMessage = new JMessage(1, null, data);
        } catch (Exception e) {
            e.printStackTrace();
            jMessage = new JMessage(0, null);
        } finally {
            closeRecoverFileItem(multipart);
        }
        return jMessage;
    }


}
