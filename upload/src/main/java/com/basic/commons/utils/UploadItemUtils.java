package com.basic.commons.utils;

import com.basic.commons.vo.FileType;
import com.basic.commons.vo.UploadItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UploadItemUtils {
    private Map<String,List<UploadItem>> UPLOAD_ITEM_MAP=new HashMap<>();

    public final static String IMAGES="images";
    public final static String AUDIOS="audios";
    public final static String VIDEOS="videos";
    public final static String OTHERS="others";

    // 保存完成个数
    private Integer successCount;
    // 全部执行个数
    private Integer total;

    public void setSuccessCount(Integer successCount) {
        this.successCount = successCount;
    }

    public Integer getSuccessCount() {
        return successCount;
    }

    public List<UploadItem> getImages() {
        return UPLOAD_ITEM_MAP.get(IMAGES);
    }

    public void addImage(UploadItem image) {
        addEntityToList(this.getImages(),image,IMAGES);
    }

    public List<UploadItem> getAudios() {
        return UPLOAD_ITEM_MAP.get(AUDIOS);
    }

    public void addAudio(UploadItem audio) {
        addEntityToList(this.getAudios(),audio,AUDIOS);
    }

    public List<UploadItem> getVideos() {
        return UPLOAD_ITEM_MAP.get(VIDEOS);
    }

    public void addVideo(UploadItem video) {
        addEntityToList(this.getVideos(),video,VIDEOS);
    }

    public List<UploadItem> getOthers() {
        return UPLOAD_ITEM_MAP.get(OTHERS);
    }

    public void addOther(UploadItem other) {
        addEntityToList(this.getOthers(),other,OTHERS);
    }

    public void addEntityToList(List<UploadItem> uploadItems,UploadItem uploadItem,String key){
        if (uploadItems==null){
            uploadItems=new ArrayList<>();
            this.UPLOAD_ITEM_MAP.put(key,uploadItems);
        }
        uploadItems.add(uploadItem);
    }

    public void addEntity(UploadItem uploadItem){
        addEntityToFileType(uploadItem.getFileType(),uploadItem);
    }

    public void addEntityToFileType(String fileType,UploadItem uploadItem){
        addEntityToFileType(FileType.getFileType(fileType),uploadItem);
    }

    public void addEntityToFileType(FileType fileType,UploadItem uploadItem){
        if (FileType.Audio == fileType){
            addAudio(uploadItem);
        } else if (FileType.Video == fileType){
            addVideo(uploadItem);
        } else if (FileType.Image == fileType){
            addImage(uploadItem);
        }else{
            addOther(uploadItem);
        }
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getTotal() {
        return total;
    }

    public static UploadItemUtils getUploadUtils(){
        return new UploadItemUtils();
    }

    /**
     * 清空计数
     */
    public void cleanCount(){
        this.setTotal(null);
        this.setSuccessCount(null);
    }
}
