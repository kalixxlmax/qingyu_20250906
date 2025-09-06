package com.basic.im.user.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.AllowRequestClient;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * 允许请求的客户端 <br>
 *
 * @author: lidaye <br>
 * @date: 2021/3/27 0027  <br>
 */
public interface AllowRequestClientDao extends IMongoDAO<AllowRequestClient, ObjectId> {


    PageResult<AllowRequestClient> queryList(int page, Integer limit, String keyword);


    List<String> queryIpList();
}
