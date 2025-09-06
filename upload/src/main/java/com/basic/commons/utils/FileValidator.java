package com.basic.commons.utils;

import cn.hutool.core.util.StrUtil;
import org.apache.commons.fileupload.FileItem;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;

/**
 * 上传文件验证
 */
public class FileValidator {

    // 不支持的文件类型
    public static final String FILE_NOT_SUPPORT="Unsupported file types!!!";

    // 黑名单列表
    private static final String blackContent = ConfigUtils.getSystemConfig().getFileBlack();
    private static final List<String> blackMimes ;

    // 白名单列表
    private static final String releaseContent = ConfigUtils.getSystemConfig().getFileRelease();
    private static final List<String> releaseMimes;

    static {
        blackMimes = StrUtil.hasBlank(blackContent) ? null : Arrays.asList(blackContent.split("\\|"));
        releaseMimes = StrUtil.hasBlank(releaseContent)? null : Arrays.asList(releaseContent.split("\\|"));
    }

    /**
     * 拦截 Servlet 请求
     */
    public static boolean checkFileSuffix(List<FileItem> multipart){
        if (multipart==null){ return true; }
        for (FileItem item : multipart) {
            if (item==null || item.isFormField() || item.getSize() < 1) {
                continue;
            }
            String suffix = ConfigUtils.getFormatName(item.getName());
            if ( null != suffix && ( isBlack(suffix) || isRelease(suffix) )  ){
                return false;
            }
        }
        return true;
    }

    /**
     * 拦截 MVC 请求
     */
    public static boolean checkFileSuffix(HttpServletRequest request){
        MultipartResolver mr = new CommonsMultipartResolver();
        if (mr.isMultipart(request)) {
            List<MultipartFile> multipartFiles = HttpRequestUtil.getMultipartFiles(request);
            for (MultipartFile multipartFile : multipartFiles) {
                if (multipartFile!=null && !multipartFile.isEmpty()){
                    String suffix = ConfigUtils.getFormatName(multipartFile.getOriginalFilename());
                    boolean isBlack = isBlack(suffix);
                    boolean isRelease = isRelease(suffix);
                    if (isBlack || isRelease){
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static boolean isBlack(String fileSuffix) {
        if (blackMimes==null ||blackMimes.size() == 0){
            return false;
        }
        return blackMimes.contains(fileSuffix.toLowerCase());
    }

    public static boolean isRelease(String fileSuffix) {
        if (releaseMimes==null || releaseMimes.size() == 0){
            return false;
        }
        return !releaseMimes.contains(fileSuffix.toLowerCase());
    }

}