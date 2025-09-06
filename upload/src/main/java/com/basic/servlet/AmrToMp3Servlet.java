package com.basic.servlet;

import java.io.File;
import java.util.List;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.Lists;
import com.basic.commons.Amr2mp3;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.vo.JMessage;
import com.basic.commons.vo.UploadItem;
import org.apache.commons.fileupload.FileItem;


@WebServlet("/upload/amrToMp3")
public class AmrToMp3Servlet extends BaseServlet {

    private static final long serialVersionUID = 1L;

    public AmrToMp3Servlet() {
        super();
    }

    @Override
    protected JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart) {
        long start = System.currentTimeMillis();
        JMessage jMessage;
        try {
            String[] paths = request.getParameterValues("paths");
            jMessage = 1 == getSystemConfig().getIsOpenfastDFS() ? fastDFSHandler(paths) : defHandler(paths);
            jMessage.put("total", paths.length);
            jMessage.put("time", System.currentTimeMillis() - start);
            return jMessage;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    private JMessage defHandler(String[] paths) {
        JMessage jMessage;
        List<UploadItem> files = Lists.newArrayList();
        int totalCount = paths.length;
        int successCount = 0;
        String path;
        UploadItem uploadItem;
        for (String s : paths) {
            path = ConfigUtils.getBasePath() + ConfigUtils.getFilePath(s);
            uploadItem = amrToMp3File(path);
            if (null != uploadItem) {
                files.add(uploadItem);
                successCount++;
            }
        }
        jMessage = new JMessage(1, null, files);
        jMessage.put("success", successCount);
        jMessage.put("failure", totalCount - successCount);
        return jMessage;
    }


    private JMessage fastDFSHandler(String[] paths) {
        JMessage jMessage;
        List<UploadItem> files = Lists.newArrayList();
        int totalCount = paths.length;
        int successCount = 0;
        String path;
        UploadItem uploadItem;
        for (String s : paths) {
            path = getSystemConfig().getFastdfsBasePath() + ConfigUtils.getFilePath(s);
            uploadItem = amrToMp3File(path);
            if (null != uploadItem) {
                files.add(uploadItem);
                successCount++;
            }
        }
        jMessage = new JMessage(1, null, files);
        jMessage.put("success", successCount);
        jMessage.put("failure", totalCount - successCount);
        return jMessage;
    }


    public UploadItem amrToMp3File(String path) {
        UploadItem uploadItem;
        if (path.endsWith(".amr")) {
            File __source = new File(path);
            File __target = new File(path.replaceAll(".amr", ".mp3"));
            if (!__target.exists()){
                Amr2mp3.changeToMp3(__source.getPath(), __target.getPath());
            }
            String url = ConfigUtils.getUrl(__target);
            uploadItem = new UploadItem(__target.getName(), url, (byte) 1, null);
            return uploadItem;
        } else {//如果文件不是  amr
            return null;
        }
    }

    public boolean deleteImage(String path) {
        boolean result = false;
        //	1fc95d99277a47f5b76d9315b8be0897.jpg
        String fileName = null;
        //		d:/data/www/resources/u/6/3000000006/201608/
        String prefixPath = null;
        int fileNameIndex = 0;
        //d:/data/www/resources/u/6/3000000006/201608/o
        File oFile = null;
        //	//d:/data/www/resources/u/6/3000000006/201608/t
        File tFile = null;
        fileNameIndex = path.lastIndexOf("/") + 1;
        fileName = path.substring(fileNameIndex);
        prefixPath = path.substring(0, fileNameIndex - 2);
        oFile = new File(prefixPath + "o/" + fileName);
        tFile = new File(prefixPath + "t/" + fileName);

        if (oFile.exists()) {
            result = oFile.delete();
            log("删除=====>" + oFile.getAbsolutePath() + "====>" + result);

        }
        if (tFile.exists()) {
            result = tFile.delete();
            log("删除=====>" + tFile.getAbsolutePath() + "====>" + result);
        }
        return result;
    }


}
