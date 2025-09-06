package com.basic.servlet;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.basic.domain.ResourceFile;
import org.apache.commons.fileupload.FileItem;

import com.google.common.base.Joiner;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FastDFSUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.commons.vo.JMessage;


@WebServlet("/upload/copyFileServlet")
public class CopyFileServlet extends BaseServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart) {
        JMessage jMessage;
        response.addHeader("Access-Control-Allow-Origin", "*");
        String paths = request.getParameter("paths");
        String childPath;
        // String validTimeStr = request.getParameter("validTime");
        try {
            childPath = FileUtils.getAbsolutePath(paths);
            if (1 == getSystemConfig().getIsOpenfastDFS() && FastDFSUtils.getGroupFormFilePath(childPath).startsWith("group")) {
                jMessage = fastDFSHandler(paths);
            } else {
                jMessage = copyProcessor(paths);
            }
            return jMessage;
        } catch (Exception e) {
            return errorMessage(e);
        }
    }

    /**
     * 文件复制
     */
    private JMessage copyProcessor(String... paths) {
        JMessage jMessage;
        int totalCount = paths.length;
        int successCount = 0;
        String path = null;
        for (String url : paths) {
            ResourceFile resourceFile = ResourcesDBUtils.getFileByUrl(url);
            if (resourceFile != null) {
                ResourcesDBUtils.incCitationsByMd5(resourceFile.getMd5(), 1);
                path = url;
                successCount = paths.length;
            }
        }
        Map<String, Object> data = new HashMap<>();
        data.put("path", path);
        data.put("url", path);
        data.put("failure", totalCount - successCount);
        data.put("success", successCount);
        jMessage = new JMessage(1, null, data);
        return jMessage;
    }


    private JMessage fastDFSHandler(String... paths) {
        JMessage jMessage;
        int totalCount = paths.length;
        int successCount = 0;
        String childPath;
        String path = null;
        String url = null;
        for (String str : paths) {
            childPath = FileUtils.getAbsolutePath(str);
            String fileName = FileUtils.getFileName(childPath);
            ResourcesDBUtils.incCitationsByFileName(fileName, 1);
            path = childPath;
            url = ConfigUtils.getFastDFSUrl(path);
            successCount++;
        }


        Map<String, Object> data = new HashMap<String, Object>();
        data.put("path", path);
        data.put("url", url);
        data.put("failure", totalCount - successCount);
        data.put("success", successCount);
        jMessage = new JMessage(1, null, data);
        log("fastDfs复制路径    " + url);
        return jMessage;
    }

    protected JMessage defHandler(String... paths) {
        JMessage jMessage;
        int totalCount = paths.length;
        int successCount = 0;
        String path = null;
        String url = null;
        for (String pathStr : paths) {
            path = copyFile(ConfigUtils.getFilePath(pathStr));
            url = ConfigUtils.getUrl(path);
        }
        Map<String, Object> data = new HashMap<>();
        data.put("path", path);
        data.put("url", url);
        data.put("failure", totalCount - successCount);
        data.put("success", successCount);
        jMessage = new JMessage(1, null, data);
        log("after the copy url " + url);
        return jMessage;
    }

    /**
     * 文件路径
     */
    private String copyFile(String childPath) {
        String path = ConfigUtils.getBasePath() + childPath;
        String fileName = FileUtils.getFileName(path);
        ResourcesDBUtils.incCitationsByFileName(fileName, 1);
        return path;
    }

    public String copyImage(String path, int isAvatar) throws IOException, Exception {
        // path=path.replace("\\", "/");// windows上文件路径
        File oldFile = new File(path);
        String result;
        String fileName;
        String formatName;
        String prefixPath;
        int fileNameIndex;
        File oFile;
        fileNameIndex = path.lastIndexOf("/") + 1;
        formatName = ConfigUtils.getFormatName(path.substring(fileNameIndex));
        // 过滤无后缀文件
        if (null == formatName)
            fileName = path.substring(fileNameIndex);
        else
            fileName = Joiner.on("").join(UUID.randomUUID().toString().replace("-", ""), ".", formatName);
        if (1 != isAvatar) {
            prefixPath = path.substring(0, fileNameIndex - 1);
            oFile = new File(prefixPath + "/" + fileName);
            // tFile=new File(prefixPath+"/"+fileName);
        } else {
            oFile = new File(path);
        }
        FileUtils.copyfile(oFile, oldFile);
        result = oFile.getPath();
        return result;
    }
}
