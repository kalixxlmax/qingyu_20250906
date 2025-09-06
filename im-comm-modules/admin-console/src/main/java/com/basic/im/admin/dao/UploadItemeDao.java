package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.UploadItem;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

/**
 * @ClassName ResourceDao
 * @Author xie yuan yuang
 * @date 2020.10.20 12:19
 * @Description
 */
public interface UploadItemeDao extends IMongoDAO<UploadItem,ObjectId> {

    PageResult<UploadItem> resourcelist(int page, int limit, String keyword, String fileType, String startTime, String endTime);

    void deleteResource(ObjectId id);

    void deleteResource(String url);
}
