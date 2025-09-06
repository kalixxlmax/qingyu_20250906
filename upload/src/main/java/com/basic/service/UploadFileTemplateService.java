package com.basic.service;

import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.commons.utils.UploadItemUtils;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.UploadFileModel;
import com.basic.commons.vo.UploadItem;
import com.basic.controller.UploadController;
import com.basic.domain.ResourceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public interface UploadFileTemplateService {

    Logger log = LoggerFactory.getLogger(UploadFileTemplateService.class);

    /**
     * 进行文件上传前的准备操作
     */
    default UploadItem doCheck(UploadFileModel model){
        try {
            if(!model.isCheckMd5()){
                /**
                 * 部分操作不校验MD5 是否重复，比如用户头像
                 */
                return null;
            }
            String fileMD5 = FileUtils.getFileMD5(model.getMultipartFile().getInputStream());
            ResourceFile resourceFile = ResourcesDBUtils.getFileByMD5(fileMD5);
            if (resourceFile!=null){

                // 是否开启色情图片过滤
                Integer openFilter = ConfigUtils.getSystemConfig().getOpenNsfwFilter();

                // 要过滤图片的阈值,0-1之间,官方建议0.85以上算色情图片
                Double nsfwImagesScore = ConfigUtils.getSystemConfig().getNsfwImagesScore();

                if (openFilter == 1) {
                    if(nsfwImagesScore!=null&&resourceFile.getNsfwScore()!=null){
                        if(resourceFile.getNsfwScore()>nsfwImagesScore){
                            //TODO  图片涉黄
                            log.info("doCheck 图片涉黄,md5值为: {}",resourceFile.getMd5());
                        }
                    }
                }

                ResourcesDBUtils.incCitationsByMd5(fileMD5,1);
                if (FileType.getFileType(resourceFile.getFileType())==FileType.Image){
                    return new UploadItem(model.getOriginalFilename(), resourceFile.getUrl(), resourceFile.getMinUrl(), (byte) 1, null,FileType.getFileType(resourceFile.getFileType()));
                }else{
                    return new UploadItem(model.getOriginalFilename(), resourceFile.getUrl(), (byte) 1, null,FileType.getFileType(resourceFile.getFileType()));
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }


    /**
     * 上传文件
     */
    UploadItem uploadFileStore(UploadFileModel uploadFileModel);

    UploadItemUtils uploadFileStoreList(UploadFileModel uploadFileModel);


   /* default UploadItemUtils saveFileStoreList(List<MultipartFile> files, int id, double validTime,String fileName,File[] uploadPath){
        if (files!=null && files.size()>0){
            return doSaveFileStoreList(files, id, validTime,fileName,uploadPath);
        }
        return null;
    }*/

    /**
     * 上传多个文件
     */
    /*default UploadItemUtils saveFileStoreList(List<MultipartFile> files, int id, double validTime){
        return saveFileStoreList(files,id,validTime,null,null);
    }*/

    /*default UploadItemUtils doSaveFileStoreList(List<MultipartFile> files, int id, double validTime,String fileName, File[] uploadPath) {
        UploadItemUtils uploadItemUtils = UploadItemUtils.getUploadUtils();
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger total=new AtomicInteger();
        files.forEach(file->{
            if (!(file==null || file.isEmpty())){
                total.incrementAndGet();
            }
            UploadItem uploadItem = saveFileStore(file, id, validTime,fileName,uploadPath);
            if (uploadItem!=null){
                successCount.getAndIncrement();
                uploadItemUtils.addEntity(uploadItem);
            }
        });
        uploadItemUtils.setTotal(total.get());
        uploadItemUtils.setSuccessCount(successCount.get());
        return uploadItemUtils;
    }*/

    default int deleteFile(List<String> filePaths){
        AtomicInteger successCount = new AtomicInteger(0);
        filePaths.forEach(obj->{
            ResourceFile resourceFile = ResourcesDBUtils.getFileByUrl(obj);
            if (resourceFile!=null){
                if (resourceFile.getCitations() > 1){
                    ResourcesDBUtils.incCitationsByMd5(resourceFile.getMd5(), -1);
                    successCount.incrementAndGet();
                }else{
                    if (deleteFile(resourceFile)){
                        successCount.incrementAndGet();
                    }
                }
            }else{
                successCount.incrementAndGet();
            }
        });
        return successCount.get();
    }

    boolean deleteFile(ResourceFile resourceFile);

}
