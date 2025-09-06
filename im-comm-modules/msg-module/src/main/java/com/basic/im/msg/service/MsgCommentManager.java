package com.basic.im.msg.service;

import com.basic.common.model.PageResult;
import com.basic.im.msg.entity.Comment;
import com.basic.im.msg.model.AddCommentParam;
import org.bson.types.ObjectId;

import java.util.List;

public interface MsgCommentManager {

    ObjectId add(int userId, AddCommentParam param);

    boolean delete(ObjectId msgId, String commentId);

    List<Comment> find(Integer userId, ObjectId msgId, ObjectId commentId, int pageIndex, int pageSize);

    PageResult<Comment> commonListMsg(ObjectId msgId, Integer page, Integer limit);
}
