package com.basic.im.company.dao;

import com.basic.im.company.entity.InviteJoinRecord;
import com.basic.mongodb.springdata.IBaseMongoRepository;
import org.bson.types.ObjectId;

public interface InviteJoinRecordDao extends IBaseMongoRepository<InviteJoinRecord, ObjectId> {


    // 添加邀请加入记录（单个）
    InviteJoinRecord addInviteJoinRecord(InviteJoinRecord inviteJoinRecord);

    InviteJoinRecord queryOne(String companyId,String departmentId, int joinUserId);
}
