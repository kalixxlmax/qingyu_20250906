package com.basic.servlet;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FastDFSUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.commons.vo.JMessage;
import com.basic.factory.UploadFileFactory;
import org.apache.commons.fileupload.FileItem;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;


@WebServlet("/upload/deleteFileServlet")
public class DeleteFileServlet extends BaseServlet {

    private static final long serialVersionUID = 1L;

    public DeleteFileServlet() {
        super();
    }

    @Override
    protected JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart) {
        long start = System.currentTimeMillis();
        response.addHeader("Access-Control-Allow-Origin", "*");
        JMessage jMessage = new JMessage();
        String[] paths = request.getParameterValues("paths");
        int successCount = UploadFileFactory.getUploadFileService().deleteFile(Arrays.asList(paths));
        jMessage.put("success", successCount);
        jMessage.put("failure", paths.length - successCount);
        jMessage.put("total", paths.length);
        jMessage.put("time", System.currentTimeMillis() - start);
        return jMessage;
    }

    private JMessage fastDFSHander(String[] paths) {
        JMessage jMessage;
        int totalCount = paths.length;
        int successCount = 0;
        String childPath;
        boolean flag;
        for (String str : paths) {
            childPath = FileUtils.getAbsolutePath(str);
            String fileName = FileUtils.getFileName(childPath);
            FindIterable<Document> findIterable = ResourcesDBUtils.getFileByFileName(fileName);
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            if (mongoCursor.hasNext()) {
                int citations = Integer.parseInt(String.valueOf(mongoCursor.next().get("citations")));
                if (citations > 1) {
                    boolean result = ResourcesDBUtils.incCitationsByFileName(fileName, -1).getModifiedCount() > 0;
                    if (result) {
                        successCount++;
                    }
                } else {
                    flag = FastDFSUtils.deleteFile(childPath);
                    ResourcesDBUtils.deleteFileByFileName(fileName);
                    if (flag) {
                        successCount++;
                    }
                }
            }
        }
        jMessage = new JMessage(1, null, "");
        jMessage.put("success", successCount);
        jMessage.put("failure", totalCount - successCount);
        return jMessage;
    }

    private JMessage defHander(String[] paths) {
        JMessage jMessage;
        int totalCount = paths.length;
        int successCount = 0;
        for (String path : paths) {
            if (FileUtils.deleteFile(ConfigUtils.getFilePath(path))){
                successCount++;
            }
        }
        jMessage = new JMessage(1, null, "");
        jMessage.put("success", successCount);
        jMessage.put("failure", totalCount - successCount);

        return jMessage;
    }


}
