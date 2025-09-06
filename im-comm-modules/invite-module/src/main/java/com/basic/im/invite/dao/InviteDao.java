package com.basic.im.invite.dao;

import com.basic.im.invite.entity.Invite;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;

public interface InviteDao extends IMongoDAO<Invite, ObjectId> {

    // 添加
    void addInvite(Invite entity);

    // 查询上级
    List<Invite.Grade> queryGradeList(int userId);

    // 查询所有下级
    List<Invite> queryInviteList(int userId);

}
