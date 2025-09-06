package com.basic.im.mpserver.dao;

import com.basic.im.mpserver.vo.MassCommentPraise;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

/**
 * 点赞相关
 */
public interface MassCommentPraiseDao extends IMongoDAO<MassCommentPraise, ObjectId> {

    MassCommentPraise sava(MassCommentPraise massCommentPraise);

    void delete(long userId,String contentId);

    MassCommentPraise findByUserId(long userId,String contentId);
}
