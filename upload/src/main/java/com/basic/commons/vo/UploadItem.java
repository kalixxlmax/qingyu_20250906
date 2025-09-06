package com.basic.commons.vo;

import com.alibaba.fastjson.annotation.JSONField;

public class UploadItem {
	@JSONField(format = "oFileName")
	private String oFileName;
	private String oUrl;
	private String tUrl;
	private Byte status;
	private String message;
	private FileType fileType;

	public UploadItem(String oFileName, String oUrl, Byte status, String message) {
		super();
		this.oFileName = oFileName;
		this.oUrl = oUrl;
		this.status = status;
		this.message = message;
	}
	public UploadItem(String oFileName, String oUrl, Byte status, String message,FileType fileType){
		this(oFileName,oUrl,status,message);
		this.fileType=fileType;
	}

	public UploadItem(String oFileName, String oUrl, String tUrl) {
		super();
		this.oFileName = oFileName;
		this.oUrl = oUrl;
		this.tUrl = tUrl;
	}

	public UploadItem(String oFileName, String oUrl, String tUrl, Byte status, String message) {
		this(oFileName,oUrl,tUrl);
		this.status = status;
		this.message = message;
	}

	public UploadItem(String oFileName, String oUrl, String tUrl, Byte status, String message,FileType fileType) {
		this(oFileName,oUrl,tUrl,status,message);
		this.fileType=fileType;
	}

	public UploadItem() {
	}

	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}

	public FileType getFileType() {
		return fileType;
	}

	public String getoFileName() {
		return oFileName;
	}

	public void setoFileName(String oFileName) {
		this.oFileName = oFileName;
	}

	public String getoUrl() {
		return oUrl;
	}

	public void setoUrl(String oUrl) {
		this.oUrl = oUrl;
	}

	public String gettUrl() {
		return tUrl;
	}

	public void settUrl(String tUrl) {
		this.tUrl = tUrl;
	}

	public Byte getStatus() {
		return status;
	}

	public void setStatus(Byte status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
