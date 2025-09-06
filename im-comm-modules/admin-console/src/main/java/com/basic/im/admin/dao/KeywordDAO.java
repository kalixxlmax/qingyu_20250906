package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.KeyWord;
import com.basic.im.admin.entity.KeywordDenyRecord;
import com.basic.mongodb.springdata.IBaseMongoRepository;
import org.bson.types.ObjectId;

import java.util.List;


public interface KeywordDAO extends IBaseMongoRepository<KeywordDenyRecord, ObjectId> {


	// 新增关键词
	void saveKeyword(KeyWord keyWord);

	// 更新关键词
	void updateKeyword(String word, ObjectId id);

	// 删除关键词
	void deleteKeyword(ObjectId id);

	// 查询关键词列表
	List<KeyWord> queryKeywordList(String word, int pageIndex, int pageSize);

	// 查询拦截消息列表
	List<KeywordDenyRecord> queryMsgInerceptList(Integer userId, String toUserId, int pageIndex, int pageSize, int type, String content);

	// 删除拦截消息
	void deleteMsgIntercept(ObjectId id);

	PageResult<KeyWord> queryKeywordPageResult(String word, int page, int limit);

	// 查询拦截消息列表
	PageResult<KeywordDenyRecord> webQueryMsgInterceptList(Integer userId, String toUserId, int pageIndex, int pageSize, int type, String content);


	long queryKeywordDenyRecordCountByType(int userId,short keywordType,short chatType);

	boolean queryByWord(String word);
}
