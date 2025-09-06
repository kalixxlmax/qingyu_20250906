package com.basic.im.msg.service.impl;

import com.basic.im.comm.utils.StringUtil;
import com.basic.im.msg.dao.FxSttingDao;
import com.basic.im.msg.entity.FxSetting;
import com.basic.im.utils.ConstantUtil;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class FxManagerImpl {

	@Autowired
	private FxSttingDao fxSttingDao;


	public List<FxSetting> queryMusicInfo(int pageIndex, int pageSize) {
		List<FxSetting> resultList = fxSttingDao.getMusicInfoList(pageIndex,pageSize);
		return resultList;
	}
	
	/**
	 * 添加短视频音乐
	 * @param musicInfo
	 */
	public void addMusicInfo(FxSetting musicInfo){
        FxSetting entity = new FxSetting();
		if (!StringUtil.isEmpty(musicInfo.getIcon())) {
            entity.setIcon(musicInfo.getIcon());
        }
		if(!StringUtil.isEmpty(musicInfo.getName())) {
            entity.setName(musicInfo.getName());
        }
		if(!StringUtil.isEmpty(musicInfo.getUrl())) {
            entity.setUrl(musicInfo.getUrl());
        }
        if (musicInfo.getSort() != null) {
            entity.setSort(musicInfo.getSort());
        }
		fxSttingDao.addMusicInfo(entity);
	}
	
	/**
	 * 删除短视频音乐
	 * @param id
	 */
	public void deleteMusicInfo(ObjectId id){
        FxSetting musicInfo = fxSttingDao.getMusicInfoById(id);
		try {
			deleteResource(musicInfo.getUrl());
            fxSttingDao.deleteMusicInfo(id);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 修改
	 * @param musicInfo
	 */
	public void updateMusicInfo(FxSetting musicInfo){
		Map<String,Object> map = new HashMap<>(5);
        if (!StringUtil.isEmpty(musicInfo.getIcon())) {
            map.put("icon",musicInfo.getIcon());
        }
        if(!StringUtil.isEmpty(musicInfo.getName())) {
            map.put("name",musicInfo.getName());
        }
        if(!StringUtil.isEmpty(musicInfo.getUrl())) {
            map.put("url",musicInfo.getUrl());
        }
        if (musicInfo.getSort() != null) {
            map.put("sort",musicInfo.getSort());
        }
        fxSttingDao.updateMusicInfo(musicInfo.getId(),map);
	}
	
	/**
	 * 删除文件
	 * @param url
	 */
	public void deleteResource(String url){
		
		ConstantUtil.deleteFile(url);
	}
}
