package com.basic.commons.vo;

public enum FileType {
	/**
	 * 文件类型
	 */
	Image("image"), Audio("audio"), Video("video"),Music("music"),MusicPhoto("music"), Other("other");

	private String baseName;

	private FileType(String baseName) {
		this.baseName = baseName;
	}

	public String getBaseName() {
		return baseName;
	}

	/**
	 * 根据名称获取对应的FileType
	 */
	public static FileType getFileType(String baseName){
		if (FileType.Image.baseName.equals(baseName)){
			return FileType.Image;
		}else if (FileType.Audio.baseName.equals(baseName)){
			return FileType.Audio;
		}else if (FileType.Video.baseName.equals(baseName)){
			return FileType.Video;
		}else{
			return FileType.Other;
		}
	}
}
