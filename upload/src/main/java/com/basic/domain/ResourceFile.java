package com.basic.domain;

public class ResourceFile {
    /**
     * 文件URL地址
     */
    private String url;

    /**
     * 缩略图存储地址
     */
    private String minUrl;
    /**
     * 存储路径
     */
    private String path;
    /**
     * 存储类型
     */
    private byte type;
    /**
     * 状态
     */
    private byte status;
    /**
     * 文件名称
     */
    private String fileName;

    private Integer citations;

    // 图片涉黄的评分,0~1,越接近1,就是色情
    private Double nsfwScore;

    /**
     * 文件MD5校验码
     */
    private String md5;
    /**
     * 文件创建时间
     */
    private long createTime;

    private long endTime;

    /**
     * 文件类型
     */
    private String fileType;



    public void setMinUrl(String minUrl) {
        this.minUrl = minUrl;
    }

    public String getMinUrl() {
        return minUrl;
    }

    public String getUrl() {
        return url;
    }


    public void setUrl(String url) {
        this.url = url;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }


    public Integer getCitations() {
        return citations;
    }

    public void setCitations(Integer citations) {
        this.citations = citations;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public Double getNsfwScore() {
        return nsfwScore;
    }

    public void setNsfwScore(Double nsfwScore) {
        this.nsfwScore = nsfwScore;
    }
}
