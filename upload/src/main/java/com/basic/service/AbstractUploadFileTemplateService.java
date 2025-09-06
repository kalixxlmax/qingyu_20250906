package com.basic.service;

import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.UploadItemUtils;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.UploadFileModel;
import com.basic.commons.vo.UploadItem;
import com.google.common.base.Joiner;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AbstractUploadFileTemplateService <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/12  <br>
 */
public abstract class AbstractUploadFileTemplateService implements UploadFileTemplateService{



    /**
     * 进行文件的保存操作
     */
    protected abstract UploadItem doSaveFileStore(UploadFileModel model);

    protected UploadItem doSaveFileStore(MultipartFile file, long id, double validTime,String fileName,File[] uploadPath){
        String suffixName = FileUtils.getSuffixName(Objects.requireNonNull(file.getOriginalFilename()));
        FileType fileType = ConfigUtils.getFileType(suffixName);
        UploadFileModel model=UploadFileModel.createUploadFileModel(file,fileType);
        model.setAttributeValues(id,fileName,validTime,uploadPath,false);
        UploadItem uploadItem = doCheck(model);
        if (uploadItem==null) {
            return doSaveFileStore(model);
        }
        return uploadItem;
    }

    @Override
    public UploadItem uploadFileStore(UploadFileModel model){
        // 防止客户端提交空的文件

        if (null==model||null==model.getMultipartFile()||null==model.getFile()||null==model.getInputStream()){
            // return null;
        }
        UploadItem uploadItem = doCheck(model);
        if (uploadItem==null) {
            return doSaveFileStore(model);
        }
        return uploadItem;
    }

    @Override
    public UploadItemUtils uploadFileStoreList(UploadFileModel model) {
        UploadItemUtils uploadItemUtils = UploadItemUtils.getUploadUtils();
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger total=new AtomicInteger();
        List<MultipartFile> multipartFileList = model.getMultipartFileList();
        for (MultipartFile file : multipartFileList) {
            if (null==file|| file.isEmpty()){
                continue;
            }
            total.incrementAndGet();
            UploadItem uploadItem = doSaveFileStore(file,model.getUserId(), model.getValidTime()
                    ,model.getFileName(),model.getUploadPath());
            if (uploadItem!=null){
                successCount.getAndIncrement();
                uploadItemUtils.addEntity(uploadItem);
            }
        }
        uploadItemUtils.setTotal(total.get());
        uploadItemUtils.setSuccessCount(successCount.get());
        return uploadItemUtils;
    }
}
