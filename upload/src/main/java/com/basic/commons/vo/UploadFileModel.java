package com.basic.commons.vo;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * 文件上传模型 <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/12  <br>
 */
public class UploadFileModel {

    private long userId;

    private double validTime=-1;

    private String fileName;

    private File[] uploadPath;

    private boolean isTranPng;

    private MultipartFile multipartFile;

    private List<MultipartFile> multipartFileList;

    /**
     * 是否上传多个文件
     */
    private boolean isMultiFiles;

    private File file;

    private InputStream inputStream;

    private FileType fileType;

    private String originalFilename;

    private String contentType;

    private String fileMd5;

    private boolean checkMd5=true;



    public UploadFileModel(MultipartFile multipartFile,FileType fileType) {
        this.multipartFile = multipartFile;
        try {
            this.inputStream=multipartFile.getInputStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.originalFilename=multipartFile.getOriginalFilename();
        this.fileType=fileType;
        this.contentType=multipartFile.getContentType();
    }

    public UploadFileModel(InputStream inputStream,FileType fileType,String originalFilename) {
        this.inputStream = inputStream;
        this.fileType=fileType;
        this.originalFilename=originalFilename;
    }

    public UploadFileModel(File file,FileType fileType,String originalFilename) {
        this.file = file;
        this.fileType=fileType;
        if(StrUtil.isEmpty(originalFilename)){
            this.originalFilename=file.getName();
        }
        this.inputStream=FileUtil.getInputStream(file);
    }
    public UploadFileModel(List<MultipartFile> multipartFileList,FileType fileType) {
        this.multipartFileList = multipartFileList;
        this.isMultiFiles=true;
        this.fileType=fileType;
    }
    public static UploadFileModel createUploadFileModel(MultipartFile multipartFile,FileType fileType){
        return new UploadFileModel(multipartFile,fileType);
    }
    public static UploadFileModel createUploadFileModel(InputStream inputStream,FileType fileType,String originalFilename){
        return new UploadFileModel(inputStream,fileType,originalFilename);
    }

    public static UploadFileModel createUploadFileModel(File file,FileType fileType,String originalFilename){
        return new UploadFileModel(file,fileType,originalFilename);
    }
    public static UploadFileModel createUploadFileModel(List<MultipartFile> multipartFileList,FileType fileType){
        return new UploadFileModel(multipartFileList,fileType);
    }



    public void setAttributeValues(long userId,String fileName, double validTime, File[] uploadPath, boolean isTranPng) {
        this.userId = userId;
        this.fileName=fileName;
        this.validTime = validTime;
        this.uploadPath = uploadPath;
        this.isTranPng = isTranPng;
    }

    public MultipartFile getMultipartFile() {
        return multipartFile;
    }

    public void setMultipartFile(MultipartFile multipartFile) {
        this.multipartFile = multipartFile;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public String getFileMd5() {
        return fileMd5;
    }

    public void setFileMd5(String fileMd5) {
        this.fileMd5 = fileMd5;
    }

    public String getOriginalFilename() {
        return originalFilename;
    }

    public void setOriginalFilename(String originalFilename) {
        this.originalFilename = originalFilename;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public double getValidTime() {
        return validTime;
    }

    public void setValidTime(double validTime) {
        this.validTime = validTime;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public File[] getUploadPath() {
        return uploadPath;
    }

    public void setUploadPath(File[] uploadPath) {
        this.uploadPath = uploadPath;
    }

    public boolean isTranPng() {
        return isTranPng;
    }

    public void setTranPng(boolean tranPng) {
        isTranPng = tranPng;
    }

    public List<MultipartFile> getMultipartFileList() {
        return multipartFileList;
    }


    public boolean isMultiFiles() {
        return isMultiFiles;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public boolean isCheckMd5() {
        return checkMd5;
    }

    public void setCheckMd5(boolean checkMd5) {
        this.checkMd5 = checkMd5;
    }
}
