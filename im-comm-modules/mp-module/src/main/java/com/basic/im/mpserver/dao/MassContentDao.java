package com.basic.im.mpserver.dao;

import com.basic.im.mpserver.vo.MassContent;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.Map;

/**
 * @Description 富文本群发相关操作
 * @Date 14:40 2020/12/14
 **/
public interface MassContentDao extends IMongoDAO<MassContent, ObjectId> {
    /**
     * 保存
     **/
    MassContent sava(MassContent massContent);

    /**
     * 查询
     **/
    MassContent find(ObjectId id);

    /**
     * 修改
     */
    void update(ObjectId id, Map<String,Object> map);

    /**
     * 修改点赞人数
     */
    void updateGiveLike(ObjectId id, int type);
}
