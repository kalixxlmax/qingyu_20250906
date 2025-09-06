package com.basic.im.admin.dao;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.UrlConfig;
import com.basic.im.admin.entity.WebUrlRoster;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface WebUrlRosterDao extends IMongoDAO<WebUrlRoster, ObjectId> {

    void addWebUrlRoster(WebUrlRoster webUrlRoster);


    Byte queryWebUrlType(String webUrl);

    PageResult<WebUrlRoster> findWebUrlRosterList(String webUrl,byte webType, int page, int limit);
}
