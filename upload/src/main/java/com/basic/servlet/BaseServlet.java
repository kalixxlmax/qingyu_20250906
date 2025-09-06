package com.basic.servlet;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.basic.commons.utils.FileValidator;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.springframework.util.StringUtils;

import com.alibaba.fastjson.JSON;
import com.basic.commons.SystemConfig;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FastDFSUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.commons.vo.FastDFSFile;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.JMessage;

/**
 * @author
 * @date 2018年5月22日
 */
public abstract class BaseServlet extends HttpServlet {


    public SystemConfig getSystemConfig() {

        return ConfigUtils.getSystemConfig();
    }

    private DiskFileItemFactory factory;

    public DiskFileItemFactory getFactory() {
        if (null == factory) {
            synchronized (this) {
                if (null == factory) {
                    factory = new DiskFileItemFactory();
                    factory.setRepository(new File(System.getProperty("java.io.tmpdir")));
                }
            }
        }
        return factory;
    }

    public String getUrl(File file) {
        return ConfigUtils.getUrl(file);
    }

    public String getFormatName(String fileName) {
        return ConfigUtils.getFormatName(fileName);
    }

    public FileType getFileType(String formatName) {
        return ConfigUtils.getFileType(formatName);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }


    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        log("=== > handle " + getClass().toString());

        request.setCharacterEncoding("UTF-8");
        response.setContentType("text/html; charset=UTF-8");

        JMessage message;
        try {
            if(!ServletFileUpload.isMultipartContent(request)) {
                message = handler(request, response,null);
                if (null == message) {
                    message = errorMessage(null);
                }
            }else {
                List<FileItem> fileItems = new ServletFileUpload(getFactory()).parseRequest(request);
                if (!FileValidator.checkFileSuffix(fileItems)) {
                    message = JMessage.errorOf(null).setResultMsg(FileValidator.FILE_NOT_SUPPORT);
                } else {
                    message = handler(request, response, fileItems);
                    if (null == message) {
                        message = errorMessage(null);
                    }
                }
            }
        } catch (Exception e) {
            log(e.getMessage(), e);
            message = new JMessage(-1, e.getMessage());
        }
        doWriter(response, message);
    }


    protected abstract JMessage handler(HttpServletRequest request, HttpServletResponse response, List<FileItem> multipart);

    protected JMessage errorMessage(Exception e) {
        return new JMessage(-1, "server error !");
    }

    protected void doWriter(HttpServletResponse response, JMessage message) {
        try {
            String s = JSON.toJSONString(message);
            PrintWriter out = response.getWriter();
            out.write(s);
            out.flush();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @param @param  item
     * @param @return 文件地址url
     * @Description: 上传文件到 fastDFS
     */
    protected String uploadToFastDFS(FileItem item, double validTime) {
        return uploadToFastDFS(item, validTime, null, null);
    }

    protected String uploadToFastDFS(FileItem item, double validTime, String md5, String fileType) {
        FastDFSFile file = new FastDFSFile();
        String fileName = StringUtils.isEmpty(item.getName()) ? item.getFieldName() : item.getName();
        file.setName(fileName);
        log("===> uploadToFastDFS upload fileName > " + fileName + " validTime > " + validTime);
        file.setSize(item.getSize());
        file.setContent(item.get());
        String path = FastDFSUtils.uploadFile(file);
        // 保存文件 URL 地址
        ResourcesDBUtils.saveFileUrl(2, path, path, validTime, md5, fileType);
        path = ConfigUtils.getFastDFSUrl(path);
        log("uploadToFastDFS uploadEd " + path);
        return path;
    }

    protected String copyFile(File file, double validTime) throws Exception {
        FastDFSFile fstfile = new FastDFSFile();
        String fileName = file.getName();
        fstfile.setName(fileName);
        log("===> uploadToFastDFS upload fileName > " + fileName + " validTime > " + validTime);
        fstfile.setSize(file.length());

        FileInputStream inputStream = new FileInputStream(file);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
        byte arr[] = new byte[1024];
        int n;
        while ((n = inputStream.read(arr)) != -1) {
            bos.write(arr, 0, n);
        }
        inputStream.close();
        byte[] data = bos.toByteArray();
        bos.close();
        fstfile.setContent(data);

        String path = FastDFSUtils.uploadFile(fstfile);
        ResourcesDBUtils.saveFileUrl(2, path, validTime);
        path = ConfigUtils.getFastDFSUrl(path);
        log("复制文件   uploadToFastDFS uploadEd " + path);
        return path;

    }

    /**
     * @param @param  multipart
     * @param @param  userId
     * @param @return 参数
     * @Description: fastdfs上传 多个文件
     */
    protected JMessage fastDFSHander(List<FileItem> multipart) {
        return null;
    }

    /**
     * @param @param  multipart
     * @param @param  userId
     * @param @return 参数
     * @Description: fastdfs上传 一个文件
     */
    protected JMessage fastDFSHander(FileItem item, double validTime) {
        return null;
    }

    /**
     * @param @param  multipart
     * @param @param  userId
     * @param @return 参数
     * @Description: 系统默认的上传 多个文件
     */
    protected JMessage defHander(List<FileItem> multipart, long userId) {
        return null;
    }

    /**
     * @param @param  multipart
     * @param @param  userId
     * @param @return 参数
     * @Description: 系统默认的上传 一个文件
     */
    protected JMessage defHander(FileItem item, long userId) {
        return null;
    }


    protected void closeRecoverFileItem(List<FileItem> multipart) {
		/*for (FileItem item : multipart) {
			try {
				item.delete();
				item.getOutputStream().flush();
				item.getOutputStream().close();

				if(null!=item.getName())
					log("close getOutputStream "+item.getName());
			}catch (Exception process){
				log(process.getMessage(),process);
			}
		}*/

    }
}

