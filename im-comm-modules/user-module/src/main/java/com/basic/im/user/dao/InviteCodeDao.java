package com.basic.im.user.dao;

import com.basic.common.model.PageResult;
import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.InviteCode;
import org.bson.types.ObjectId;

public interface InviteCodeDao extends IMongoDAO<InviteCode, ObjectId> {

    void addInviteCode(InviteCode inviteCode);

    InviteCode findUserInviteCode(int userId);

    PageResult<InviteCode> getInviteCodeList(int userId, String keyworld, short status, int pageIndex, int pageSize);

    boolean deleteInviteCode(int userId,ObjectId inviteCodeId);

    InviteCode findInviteCodeByCode(String inviteCode);


    void updateUserInviteCode(int userId,String inviteCode);
}
