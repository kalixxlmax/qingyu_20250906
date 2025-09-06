package com.basic.service;

import org.springframework.web.multipart.MultipartFile;

import java.io.File;

/**
 * @Description:
 * @Author weiXiangMing
 * @Date 2021/11/10 16:10
 */
public interface CheckImagesService {
    /*
     *
     * @param file
     * @param oUrl 大图
     * @param tUrl 缩略图
     * @return void
     */
    void CheckImages(MultipartFile file,String oUrl,String tUrl);
    void CheckImages(File file ,String oUrl,String tUrl);
}
