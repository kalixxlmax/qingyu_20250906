package com.basic.im.user.service;

import com.basic.common.model.PageResult;
import com.basic.im.user.entity.AllowRequestClient;
import org.bson.types.ObjectId;

import java.util.Set;

/**
 * AllowRequestClient <br>
 *
 * @author: lidaye <br>
 * @date: 2021/3/29 0029  <br>
 */
public interface AllowRequestClientService {


    PageResult<AllowRequestClient> queryList(int page, Integer limit, String keyword);


    void updateAllowRequest(AllowRequestClient requestClient);


    void deleteAllowRequest(ObjectId id);


    Set<String> queryIpList();

    boolean isAllowRequest(String ip);
}
