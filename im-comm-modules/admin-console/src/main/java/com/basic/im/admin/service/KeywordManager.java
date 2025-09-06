package com.basic.im.admin.service;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.KeyWord;
import com.basic.im.admin.entity.KeywordDenyRecord;
import org.bson.types.ObjectId;

import java.util.List;

public interface KeywordManager {
	
	public void addKeyword(String word, String id);

	public void deleteKeyword(ObjectId id);

	void  sendRefreshSysConfig();

	public List<KeyWord> queryKeywordList(String word, int pageIndex, int pageSize);

	public List<KeywordDenyRecord> queryKeywordDenyRecordList(Integer userId, String toUserId, int pageIndex, int pageSize, int type, String content);
	
	public void deleteMsgIntercept(ObjectId id);

	public PageResult<KeywordDenyRecord> webQueryKeywordDenyRecordList(Integer userId, String toUserId, int pageIndex, int pageSize, int type, String content);
}
