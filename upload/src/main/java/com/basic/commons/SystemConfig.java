package com.basic.commons;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Configuration
@ConfigurationProperties
public  class SystemConfig {


	private  int beginIndex;
	private  String domain;
	private  int isBackDomain=1;//上传的文件是否返回域名  默认返回   1:返回    0：不返回

	/**
	 * 色情图片鉴别
	 */
	// 是否开启色情图片过滤 .0 不开启,1开启
	private  Integer openNsfwFilter=0;
	// 要过滤图片的阈值,0-1之间,官方建议0.85以上算色情图片
	private  Double nsfwImagesScore=0.85;
	// Python 的NSFW的鉴别模块的IP地址
	private  String nsfwModuleUrl;

	/**
	 * MINIO 上传文件配置
	 */
	// IP地址
	private  String minioEndpoint;
	// 文件桶名称
	private  String minioBucketName;
	// AccessKey
	private  String minioAccessKey;
	// SecretKey
	private  String minioSecretKey;

	private String minioImageDomain;

	/**
	 * 文件 保存数据库的 uri
	 */
	private  String dbUri;

	private  String dbName = "resources";

	//需要删除的文件对应数据库的url
	private String delFileUri;

	private  int isOpenfastDFS=0; // 是否开启fastDFs 文件系统

	private  String uploadTypeName="local"; // 文件存储类型

	private  String fastdfsDomain;

	public String fastdfsBasePath;




	/**
	 * redis 配置
	 */
	private String redis_addr;

	private int redis_database = 0;

	private String redis_password;

	private short redis_isCluster=0; //是否开启集群 0 关闭 1开启

	private int redis_connectionMinimumIdleSize=32;

	private int redis_connectionPoolSize=64;

	private int redis_connectTimeout=10000;

	private int redis_pingConnectionInterval=500;

	private int redis_timeout=10000;


	/**
	 * 开启定时任务   删除文件
		  0  关闭     1 开启
		 在部署 多个 upload 项目的情况下
		只需要 一个 项目 执行定时任务就可以了
	 */
	private  int openTask=0;

	private  String basePath;

	private  String uTemp;
	private  String nTemp;
	private  String oTemp;
	private  String tTemp;

	private  String imageFilter;
	private  String audioFilter;
	private  String videoFilter;

	private  int amr2mp3;

	private int thumbnailSize=100;


	private String cosAccessKey;
	private String cosSecretKey;
	private String cosRegion;
	private String cosImageDomain;
	private String cosBucket;


	private String ossEndpoint;
	private String ossAccessKey;
	private String ossSecretKey;
	private String ossImageDomain;
	private String ossBucket;


	private String fileBlack;
	private String fileRelease;


	public String getFileBlack() {
		return fileBlack;
	}

	public void setFileBlack(String fileBlack) {
		this.fileBlack = fileBlack;
	}

	public String getFileRelease() {
		return fileRelease;
	}

	public void setFileRelease(String fileRelease) {
		this.fileRelease = fileRelease;
	}

	public String getCosBucket() {
		return cosBucket;
	}

	public void setCosBucket(String cosBucket) {
		this.cosBucket = cosBucket;
	}

	/**
	 * 避免用户配置域名时没有加入 / 作为后缀
	 */
	public String getCosImageDomain() {
		String regex="/";
		if (!cosImageDomain.endsWith(regex)){
			cosImageDomain = cosImageDomain + regex;
		}
		return cosImageDomain;
	}

	public void setCosImageDomain(String cosImageDomain) {
		this.cosImageDomain = cosImageDomain;
	}

	public String getUploadTypeName() {
		return uploadTypeName;
	}

	public void setUploadTypeName(String uploadTypeName) {
		this.uploadTypeName = uploadTypeName;
	}

	public String getAudioFilter() {
		return audioFilter;
	}

	public void setAudioFilter(String audioFilter) {
		this.audioFilter = audioFilter;
	}

	public int getBeginIndex() {
		return beginIndex;
	}

	public void setBeginIndex(int beginIndex) {
		this.beginIndex = beginIndex;
	}

	public String getDomain() {
		return domain;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public int getIsBackDomain() {
		return isBackDomain;
	}

	public void setIsBackDomain(int isBackDomain) {
		this.isBackDomain = isBackDomain;
	}

	public String getDbUri() {
		return dbUri;
	}

	public void setDbUri(String dbUri) {
		this.dbUri = dbUri;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDelFileUri() { return delFileUri; }

	public void setDelFileUri(String delFileUri) { this.delFileUri = delFileUri; }

	public int getIsOpenfastDFS() {
		return isOpenfastDFS;
	}

	public void setIsOpenfastDFS(int isOpenfastDFS) {
		this.isOpenfastDFS = isOpenfastDFS;
	}

	public String getFastdfsDomain() {
		return fastdfsDomain;
	}

	public void setFastdfsDomain(String fastdfsDomain) {
		this.fastdfsDomain = fastdfsDomain;
	}

	public int getOpenTask() {
		return openTask;
	}

	public void setOpenTask(int openTask) {
		this.openTask = openTask;
	}

	public String getImageFilter() {
		return imageFilter;
	}

	public void setImageFilter(String imageFilter) {
		this.imageFilter = imageFilter;
	}

	public String getnTemp() {
		return nTemp;
	}

	public void setnTemp(String nTemp) {
		this.nTemp = nTemp;
	}

	public String getoTemp() {
		return oTemp;
	}

	public void setoTemp(String oTemp) {
		this.oTemp = oTemp;
	}

	public String gettTemp() {
		return tTemp;
	}

	public void settTemp(String tTemp) {
		this.tTemp = tTemp;
	}

	public String getuTemp() {
		return uTemp;
	}

	public void setuTemp(String uTemp) {
		this.uTemp = uTemp;
	}

	public String getVideoFilter() {
		return videoFilter;
	}

	public void setVideoFilter(String videoFilter) {
		this.videoFilter = videoFilter;
	}

	public int getAmr2mp3() {
		return amr2mp3;
	}

	public void setAmr2mp3(int amr2mp3) {
		this.amr2mp3 = amr2mp3;
	}

	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}

	/**
	* @return basePath
	*/
	public String getBasePath() {
		return basePath;
	}


	public String getFastdfsBasePath() {
		return fastdfsBasePath;
	}

	public void setFastdfsBasePath(String fastdfsBasePath) {
		this.fastdfsBasePath = fastdfsBasePath;
	}

	public String getRedis_addr() {
		return redis_addr;
	}

	public void setRedis_addr(String redis_addr) {
		this.redis_addr = redis_addr;
	}

	public int getRedis_database() {
		return redis_database;
	}

	public void setRedis_database(int redis_database) {
		this.redis_database = redis_database;
	}

	public String getRedis_password() {
		return redis_password;
	}

	public void setRedis_password(String redis_password) {
		this.redis_password = redis_password;
	}

	public short getRedis_isCluster() {
		return redis_isCluster;
	}

	public void setRedis_isCluster(short redis_isCluster) {
		this.redis_isCluster = redis_isCluster;
	}

	public int getRedis_connectionMinimumIdleSize() {
		return redis_connectionMinimumIdleSize;
	}

	public void setRedis_connectionMinimumIdleSize(int redis_connectionMinimumIdleSize) {
		this.redis_connectionMinimumIdleSize = redis_connectionMinimumIdleSize;
	}

	public int getRedis_connectionPoolSize() {
		return redis_connectionPoolSize;
	}

	public void setRedis_connectionPoolSize(int redis_connectionPoolSize) {
		this.redis_connectionPoolSize = redis_connectionPoolSize;
	}

	public int getRedis_connectTimeout() {
		return redis_connectTimeout;
	}

	public void setRedis_connectTimeout(int redis_connectTimeout) {
		this.redis_connectTimeout = redis_connectTimeout;
	}

	public int getRedis_pingConnectionInterval() {
		return redis_pingConnectionInterval;
	}

	public void setRedis_pingConnectionInterval(int redis_pingConnectionInterval) {
		this.redis_pingConnectionInterval = redis_pingConnectionInterval;
	}

	public int getRedis_timeout() {
		return redis_timeout;
	}

	public void setRedis_timeout(int redis_timeout) {
		this.redis_timeout = redis_timeout;
	}
	public int getThumbnailSize() {
		return thumbnailSize;
	}

	public void setThumbnailSize(int thumbnailSize) {
		this.thumbnailSize = thumbnailSize;
	}

	public String getCosAccessKey() {
		return cosAccessKey;
	}

	public void setCosAccessKey(String cosAccessKey) {
		this.cosAccessKey = cosAccessKey;
	}

	public String getCosSecretKey() {
		return cosSecretKey;
	}

	public void setCosSecretKey(String cosSecretKey) {
		this.cosSecretKey = cosSecretKey;
	}

	public String getCosRegion() {
		return cosRegion;
	}

	public void setCosRegion(String cosRegion) {
		this.cosRegion = cosRegion;
	}

	public String getOssEndpoint() {
		return ossEndpoint;
	}

	public void setOssEndpoint(String ossEndpoint) {
		this.ossEndpoint = ossEndpoint;
	}

	public String getOssAccessKey() {
		return ossAccessKey;
	}

	public void setOssAccessKey(String ossAccessKey) {
		this.ossAccessKey = ossAccessKey;
	}

	public String getOssSecretKey() {
		return ossSecretKey;
	}

	public void setOssSecretKey(String ossSecretKey) {
		this.ossSecretKey = ossSecretKey;
	}


	/**
	 * 避免用户配置域名时没有加入 / 作为后缀
	 */
	public String getOssImageDomain() {
		String regex="/";
		if (!ossImageDomain.endsWith(regex)){
			ossImageDomain = ossImageDomain + regex;
		}
		return ossImageDomain;
	}

	public void setOssImageDomain(String ossImageDomain) {
		this.ossImageDomain = ossImageDomain;
	}

	public String getOssBucket() {
		return ossBucket;
	}

	public void setOssBucket(String ossBucket) {
		this.ossBucket = ossBucket;
	}

	public Integer getOpenNsfwFilter() {
		return openNsfwFilter;
	}

	public void setOpenNsfwFilter(Integer openNsfwFilter) {
		this.openNsfwFilter = openNsfwFilter;
	}

	public String getNsfwModuleUrl() {
		return nsfwModuleUrl;
	}

	public void setNsfwModuleUrl(String nsfwModuleUrl) {
		this.nsfwModuleUrl = nsfwModuleUrl;
	}

	public Double getNsfwImagesScore() {
		return nsfwImagesScore;
	}

	public void setNsfwImagesScore(Double nsfwImagesScore) {
		this.nsfwImagesScore = nsfwImagesScore;
	}

	public String getMinioEndpoint() {
		return minioEndpoint;
	}

	public void setMinioEndpoint(String minioEndpoint) {
		this.minioEndpoint = minioEndpoint;
	}

	public String getMinioBucketName() {
		return minioBucketName;
	}

	public void setMinioBucketName(String minioBucketName) {
		this.minioBucketName = minioBucketName;
	}

	public String getMinioAccessKey() {
		return minioAccessKey;
	}

	public void setMinioAccessKey(String minioAccessKey) {
		this.minioAccessKey = minioAccessKey;
	}

	public String getMinioSecretKey() {
		return minioSecretKey;
	}

	public void setMinioSecretKey(String minioSecretKey) {
		this.minioSecretKey = minioSecretKey;
	}

	public String getMinioImageDomain() {
		return minioImageDomain;
	}

	public void setMinioImageDomain(String minioImageDomain) {
		this.minioImageDomain = minioImageDomain;
	}
}
