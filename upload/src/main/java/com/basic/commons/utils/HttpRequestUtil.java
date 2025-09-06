package com.basic.commons.utils;

import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HTTP Request 工具类
 */
public class HttpRequestUtil {

    /**
     * 获取所有上传文件
     */
    public static List<MultipartFile> getMultipartFiles(HttpServletRequest request){
        return getMultipartFiles(request,"files");
    }

    public static List<MultipartFile> getMultipartFiles(HttpServletRequest request,String fileName){
        if(request instanceof MultipartHttpServletRequest) {
            MultipartHttpServletRequest multipartHttpServletRequest=(MultipartHttpServletRequest) request;
            return multipartHttpServletRequest.getFiles(fileName);
        }
        return null;
    }


    /**
     * 获取单个上传文件
     */
    public static MultipartFile getMultipartFile(HttpServletRequest request){
        List<MultipartFile> multipartFiles = getMultipartFiles(request);
        AtomicReference<MultipartFile> multipartFile=new AtomicReference<>();
        if (multipartFiles!=null){
            multipartFiles.forEach(file -> {
                if (!file.isEmpty()){
                    multipartFile.set(file);
                }
            });
        }
        return multipartFile.get();
    }
}
