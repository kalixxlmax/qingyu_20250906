package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.PushNews;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

/**
 * @ClassName PushConfigDao
 * @Author xie yuan yuang
 * @date 2020.08.03 12:23
 * @Description
 */
public interface PushNewsDao extends IMongoDAO<PushNews, ObjectId> {

    void sava(PushNews pushNews);

    PageResult<PushNews> getPushNewsList(String startTime, String endTime, int page, int limit, String type,String content);

    void deletePushNews(ObjectId id);
}
