package com.basic.controller;

import cn.hutool.core.codec.Base64;
import com.alibaba.fastjson.JSONObject;
import com.basic.commons.vo.UploadFileModel;
import com.google.common.base.Joiner;
import com.qcloud.cos.utils.Md5Utils;
import com.basic.commons.utils.*;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.JMessage;
import com.basic.commons.vo.UploadItem;
import com.basic.domain.ResourceFile;
import com.basic.factory.UploadFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.time.chrono.IsoChronology;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 文件上传 Controller
 */
@Controller
public class UploadController {

	private static final Logger log = LoggerFactory.getLogger(UploadController.class);

	/**
	 * 测试页面
	 */
	@RequestMapping("/")
	public String  index() {
		return "index";
	}

	/**
	 * 检测上传文件 MD5,返回重复的文件名称
	 */
	@ResponseBody
	@RequestMapping("/file/v1/check/md5")
	public JMessage checkFileMD5(@RequestParam(value = "md5Code")String md5Code){
		ResourceFile resourceFile = ResourcesDBUtils.getFileByMD5(md5Code);
		if (resourceFile!=null){
			ResourcesDBUtils.incCitationsByMd5(md5Code,1);
			return JMessage.valueOf(resourceFile.getUrl());
		}
		return JMessage.valueOf(null);
	}

	@ResponseBody
	@RequestMapping("/file/v1/check/md5/multi")
	public JMessage checkFileMD5Multi(@RequestParam(value = "md5Codes") List<String> md5Codes){
		List<String> resultData=new ArrayList<>();
		md5Codes.forEach(md5Code->{
			ResourceFile resourceFile = ResourcesDBUtils.getFileByMD5(md5Code);
			if (resourceFile!=null){
				ResourcesDBUtils.incCitationsByMd5(md5Code,1);
				resultData.add(resourceFile.getUrl());
			}
		});
		return JMessage.valueOf(resultData);
	}

	/**
	 * 商务圈分类图片上传
	 */
	@ResponseBody
	@RequestMapping("/upload/categoryServlet")
//	@RequestMapping("/file/v1/category/upload")
	public JMessage uploadCategoryFile(HttpServletRequest request,
									   @RequestParam(name = "categoryId",defaultValue = "0") Integer categoryId){
		String fileName = categoryId + ".jpg";
		File[] uploadPath = ConfigUtils.getCategoryPath();
		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(HttpRequestUtil.getMultipartFiles(request),FileType.Image);
		uploadFileModel.setAttributeValues(categoryId.longValue(),fileName,-1,uploadPath,false);

		UploadItemUtils uploadItemUtils=UploadFileFactory.getUploadFileService().uploadFileStoreList(uploadFileModel);
		return new JMessage(1, null, uploadItemUtils).put("success", uploadItemUtils.getSuccessCount());
	}

	/**
	 * 上传群组自定义头像
	 */
	@ResponseBody
	@RequestMapping("/upload/groupAvatarServlet")
//	@RequestMapping("/file/v1/group/avatar/upload")
	public JMessage uploadGroupAvatarFile(HttpServletRequest request,
										  @RequestParam(name = "jid") String jid){
		MultipartFile file=HttpRequestUtil.getMultipartFile(request);
		if (null==file || file.isEmpty()){ return new JMessage(1010101, "缺少上传文件"); }
		if (StringUtils.isEmpty(jid)){ return new JMessage(1010101, "缺少请求参数"); }
		File[] uploadPath = ConfigUtils.getGroupAvatarPath(jid);
		String fileName = Joiner.on("").join(jid, ".", "jpg");
		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(file,FileType.Image);
		uploadFileModel.setAttributeValues(0,fileName,-1,uploadPath,false);
		uploadFileModel.setCheckMd5(false);
		UploadItem uploadItem = UploadFileFactory.getUploadFileService().uploadFileStore(uploadFileModel);
		if(null!=uploadItem){
			ResourcesDBUtils.saveGroupAvatarFile(jid,true);
		}
		return new JMessage(1, null, uploadItem);
	}

	/**
	 * 更新群组头像用户列表
	 */
	@ResponseBody
	@RequestMapping("/upload/updateGroupAvatar")
	public JMessage updateGroupAvatar(HttpServletRequest request,
										  @RequestParam(name = "jid") String jid,
									  @RequestParam(name = "userIds") String userIds){

		if (StringUtils.isEmpty(jid)){ return new JMessage(1010101, "缺少请求参数"); }
		if(ResourcesDBUtils.getGroupAvatarIsExists(jid)){
			return new JMessage(1,null);
		}
		File[] uploadPath = ConfigUtils.getGroupAvatarPath(jid);
		String fileName = Joiner.on("").join(jid, ".", "jpg");
		List<Long> userIdList = com.basic.commons.utils.StringUtils.getLongList(userIds, ",");
		while (userIdList.size()>5){
			userIdList.remove(userIdList.size()-1);
		}
		List<File> fileList = ConfigUtils.getAvatarLocalPath(userIdList);
		InputStream inputStream =null;
		try {
			inputStream=ImageUtil.getCombinationOfHead(fileList, uploadPath[0].getPath());
			UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(inputStream,FileType.Image,null);
			uploadFileModel.setAttributeValues(0,fileName,-1,uploadPath,false);

			if(null!=inputStream) {
				uploadFileModel.setCheckMd5(false);
				UploadItem uploadItem = UploadFileFactory.getUploadFileService().uploadFileStore(uploadFileModel);

				return new JMessage(1, null, uploadItem);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new JMessage(0, null, null);


	}

	/**
	 * 自定义头像上传
	 */
	@ResponseBody
	@RequestMapping("/upload/uploadAvatarServlet")
//	@RequestMapping("/file/v1/avatar/upload")
	public JMessage uploadAvatarFile(HttpServletRequest request,
									 @RequestParam(name = "userId",defaultValue = "0") Integer userId){
		MultipartFile file=HttpRequestUtil.getMultipartFile(request);
		if (null==file || file.isEmpty()){ return new JMessage(1010101, "缺少上传文件"); }
		if (userId==0){ return new JMessage(1010101, "缺少请求参数"); }
		File[] uploadPath = ConfigUtils.getAvatarPath(userId);
		String fileName =ConfigUtils.getAvatarFileName(userId.toString());

		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(file,FileType.Image);
		uploadFileModel.setAttributeValues(userId,fileName,-1,uploadPath,true);

		uploadFileModel.setCheckMd5(false);
		UploadItem uploadItem = UploadFileFactory.getUploadFileService(UploadFileFactory.LOCAL_TYPE).uploadFileStore(uploadFileModel);
		try {
			ResourcesDBUtils.incCitationsByMd5(FileUtils.getFileMD5(file.getInputStream()),1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		log.info("uploadItem value is {}",JSONObject.toJSONString(uploadItem));
		return new JMessage(1, null, uploadItem);
	}

	/**
	 * 头像上传
	 */
	@ResponseBody
	@RequestMapping("/upload/uploadAvatarServletByIds")
//	@RequestMapping("/file/v1/headImg/upload")
	public JMessage uploadHeadImageFile(MultipartFile image,
										@RequestParam(name = "userId",defaultValue = "0") Integer id,
										@RequestParam(name = "version",defaultValue = "0") Integer version){
		if (null==image || image.isEmpty()){
			return new JMessage(1020101, "缺少上传文件");
		} else if (0 == id || 0 == version) {
			return new JMessage(1010101, "缺少请求参数");
		}
		List<Integer> userIds = new ArrayList<>();
		userIds.add(id);
		if (1 == version) {
			userIds.add(id + 1);
		} else {
			userIds.add(id - 1);
		}
		List<UploadItem> resultData = new ArrayList<>();
		userIds.forEach(userId->{
			File[] uploadPath = ConfigUtils.getAvatarPath(userId);
			String fileName =ConfigUtils.getAvatarFileName(userId.toString());
			UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(image,FileType.Image);
			uploadFileModel.setAttributeValues(userId,fileName,-1,uploadPath,true);

			resultData.add(UploadFileFactory.getUploadFileService().uploadFileStore(uploadFileModel));
			try {
				ResourcesDBUtils.incCitationsByMd5(FileUtils.getFileMD5(image.getInputStream()),1);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		return JMessage.valueOf(resultData);
	}


	/**
	 * 头像上传
	 */
	@ResponseBody
	@RequestMapping("/upload/uploadifyAvatarServlet")
//	@RequestMapping("/file/v1/uploadify/avatar/upload")
	public JMessage uploadUploadifyAvatarFile(MultipartFile image,
											  @RequestParam(name = "userId",defaultValue = "0") Integer userId){
		if (null==image || image.isEmpty()){ return new JMessage(1020101, "缺少上传文件"); }
		if (0==userId){return new JMessage(1010101, "缺少请求参数");}
		String fileName = ConfigUtils.getAvatarFileName(userId.toString());
		File[] uploadPath = ConfigUtils.getAvatarPath(userId);
		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(image,FileType.Image);
		uploadFileModel.setAttributeValues(userId,fileName,-1,uploadPath,true);
		UploadItem uploadItem = UploadFileFactory.getUploadFileService(UploadFileFactory.LOCAL_TYPE).uploadFileStore(uploadFileModel);
		try {
			ResourcesDBUtils.incCitationsByMd5(FileUtils.getFileMD5(image.getInputStream()),1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		log.info("uploadItem value is {}",JSONObject.toJSONString(uploadItem));
		return new JMessage(1, null,uploadItem);
	}

	/**
	 * 文件上传
	 */
	@ResponseBody
	@RequestMapping("/upload/uploadifyServlet")
//	@RequestMapping("/file/v1/uploadify/upload")
	public JMessage uploadUploadifyFile(HttpServletRequest request,
										@RequestParam(name = "userId",defaultValue = "0") Integer userId,
										@RequestParam(name = "validTime",defaultValue = "-1") Double validTime){
		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(HttpRequestUtil.getMultipartFile(request),null);
		uploadFileModel.setAttributeValues(userId,null,-1,null,false);
		UploadItem uploadItem = UploadFileFactory.getUploadFileService().uploadFileStore(uploadFileModel);
		return new JMessage(1, null).put("url", ConfigUtils.getFullUrl(uploadItem.getoUrl()));
	}

	/**
	 * 音乐上传
	 */
	@ResponseBody
	@RequestMapping("/upload/uploadMusicServlet")
//	@RequestMapping("/file/v1/music/upload")
	public JMessage uploadMusicFile(HttpServletRequest request){
		MultipartFile multipartFile = HttpRequestUtil.getMultipartFile(request);
		String oFileName = multipartFile.getOriginalFilename();
		String suffixName = FileUtils.getSuffixName(Objects.requireNonNull(oFileName));
		String fileName = 32 == ConfigUtils.getName(oFileName).length() ? oFileName : Joiner.on("").join(UUID.randomUUID().toString().replace("-", ""), ".", suffixName);
		FileType fileType = FileType.getFileType(suffixName);
		if (fileType == FileType.Image) {
			fileType = FileType.MusicPhoto;
			oFileName=fileName;
		} else {
			fileType = FileType.Music;
		}
		File[] uploadPath = ConfigUtils.getUploadPath(fileType);
		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(HttpRequestUtil.getMultipartFile(request),fileType);
		uploadFileModel.setAttributeValues(0,oFileName,-1,uploadPath,false);

		UploadItem uploadItem = UploadFileFactory.getUploadFileService().uploadFileStore(uploadFileModel);
		return new JMessage(1, null,uploadItem.getoUrl());
	}


	/**
	 * 常用上传
	 */
	@ResponseBody
	@RequestMapping("/upload/uploadServlet")
//	@RequestMapping("/file/v1/upload")
	public JMessage uploadFile(HttpServletRequest request,
							   @RequestParam(name = "userId",defaultValue = "0") Integer userId,
							   @RequestParam(name = "validTime",defaultValue = "-1") Double validTime,
							   @RequestParam(name = "uploadFlag",defaultValue = "0") Integer uploadFlag){
		List<MultipartFile> multipartFiles = HttpRequestUtil.getMultipartFiles(request);

		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(multipartFiles,null);
		uploadFileModel.setAttributeValues(userId,null,-1,null,false);

		UploadItemUtils uploadItemUtils=UploadFileFactory.getUploadFileService().uploadFileStoreList(uploadFileModel);
		if (uploadItemUtils!=null){
			JMessage jMessage = new JMessage(1, null, uploadItemUtils);
			jMessage.put("success", uploadItemUtils.getSuccessCount());
			jMessage.put("total",uploadItemUtils.getTotal());
			jMessage.put("failure",uploadItemUtils.getTotal() - uploadItemUtils.getSuccessCount());
			// 清空多余计数
			uploadItemUtils.cleanCount();
			return jMessage;
		}
		return new JMessage(0,"files is null!!!");
	}

	/**
	 * 音频上传
	 */
	@ResponseBody
	@RequestMapping("/upload/uploadVoiceServlet")
//	@RequestMapping("/file/v1/voice/upload")
	public JMessage uploadVoiceFile(HttpServletRequest request, @RequestParam(name = "userId",defaultValue = "0") Integer userId){
		if (userId==0){ return new JMessage(1010101, "缺少请求参数"); }
		File[] uploadPath = ConfigUtils.getUploadPath(userId, FileType.Audio);
		String suffixName ="wav";
		String fileName = Joiner.on("").join(UUID.randomUUID().toString().replace("-", ""), ".", suffixName);

		UploadFileModel uploadFileModel=UploadFileModel.createUploadFileModel(HttpRequestUtil.getMultipartFile(request),FileType.Audio);
		uploadFileModel.setAttributeValues(userId,fileName,-1,uploadPath,false);

		UploadItem uploadItem = UploadFileFactory.getUploadFileService().uploadFileStore(uploadFileModel);
		return new JMessage(1, null).put("url", uploadItem.getoUrl());
	}

}

