package com.basic.service.impl;

import cn.hutool.http.HttpUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.log.level.Level;
import com.basic.commons.SystemConfig;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.domain.ResourceFile;
import com.basic.service.CheckImagesService;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * @Description:
 * @Author weiXiangMing
 * @Date 2021/11/10 16:11
 */
@Service
public class CheckImagesServiceImpl implements CheckImagesService {


    Log log = LogFactory.get();


    @Override
    public void CheckImages(MultipartFile file, String oUrl, String tUrl) {


        check(file, null, oUrl, tUrl);

    }


    // 只能识别  MultipartFile File
    public void check(MultipartFile multipartFile, File file,String oUrl, String tUrl) {
        Long systemTime = System.currentTimeMillis();

        // 是否开启色情图片过滤
        Integer openFilter = ConfigUtils.getSystemConfig().getOpenNsfwFilter();

        // 要过滤图片的阈值,0-1之间,官方建议0.85以上算色情图片
        Double nsfwImagesScore = ConfigUtils.getSystemConfig().getNsfwImagesScore();

        // Python的图片鉴黄模块
        final String nsfwModuleUrl = ConfigUtils.getSystemConfig().getNsfwModuleUrl();

        if (openFilter == 1) {
            //获取文件的原始名

            HashMap<String, Object> paramMap = new HashMap<>();
            try {

                if (multipartFile != null) {
                    String filename = multipartFile.getOriginalFilename();
                    paramMap.put("file", multipartFileToFile(multipartFile));
                }

                if (file != null) {
                    String filename = file.getName();
                    paramMap.put("file", file);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> {

                String result = HttpUtil.post(nsfwModuleUrl, paramMap);

                Double score = Double.parseDouble(result);
                if (score > nsfwImagesScore) {
                    log.info("敏感图片 " + score, Level.INFO);
                    // 保存分值
                    ResourcesDBUtils.updateImagesNSFW(oUrl, score);

                } else {
                    log.info("不是敏感图片 " + score, Level.INFO);
                    ResourcesDBUtils.updateImagesNSFW(oUrl, score);
                }
                return "success";
            });
        }
    }

    @Override
    public void CheckImages(File file, String oUrl, String tUrl) {


    }


    public static File multipartFileToFile(MultipartFile file) throws Exception {

        File toFile = null;
        if (file.equals("") || file.getSize() <= 0) {
            file = null;
        } else {
            InputStream ins = null;
            ins = file.getInputStream();
            toFile = new File(file.getOriginalFilename());
            inputStreamToFile(ins, toFile);
            ins.close();
        }
        return toFile;
    }

    //获取流文件
    private static void inputStreamToFile(InputStream ins, File file) {
        try {
            OutputStream os = new FileOutputStream(file);
            int bytesRead = 0;
            byte[] buffer = new byte[8192];
            while ((bytesRead = ins.read(buffer, 0, 8192)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            os.close();
            ins.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





/*    // 是否开启色情图片过滤
    Boolean openFilter=false;

    // 要过滤图片的阈值,0-1之间,官方建议0.85以上算色情图片
    Double filterThreshold=0.8;

    Long systemTime = System.currentTimeMillis();

        if(openFilter){
        //获取文件的原始名
        HashMap<String, Object> paramMap = new HashMap<>();
        try {
            paramMap.put("file", file);
        } catch (Exception process) {
            process.printStackTrace();
        }
        CompletableFuture<String> future2 = CompletableFuture.supplyAsync(() -> {

            String result = HttpUtil.post("192.168.0.170:9800", paramMap);

            Double randomId = Double.parseDouble(result);
            if (randomId > filterThreshold) {
                log.info("敏感图片 "+randomId, Level.INFO);
            } else {
                log.info("不是敏感图片 "+randomId, Level.INFO);
            }
            return "success";
        });
    }*/
}
