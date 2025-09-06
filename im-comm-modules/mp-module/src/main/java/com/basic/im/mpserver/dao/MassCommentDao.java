package com.basic.im.mpserver.dao;

import com.basic.im.mpserver.vo.MassComment;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * @Description 评论相关操作
 * @Date 14:40 2020/12/14
 **/
public interface MassCommentDao extends IMongoDAO<MassComment, ObjectId> {
    /**
     * 保存
     **/
    MassComment sava(MassComment massComment);

    /**
     * 查询内容下全部评论
     **/
    List<MassComment> find(String id, int pageIndex, int pageSize);

    /**
     * 查询内容下全部评论数量
     **/
    long countMassComment(String id);


    /**
     * 修改点赞数量
     **/
    void updateGiveLike(String commentId , int type);
}
