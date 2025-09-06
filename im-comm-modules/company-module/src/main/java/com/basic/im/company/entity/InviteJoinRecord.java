package com.basic.im.company.entity;


import com.basic.im.comm.utils.DateUtil;
import io.swagger.annotations.ApiModel;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@ApiModel("邀请加入记录")
@Document(value = "inviteJoin")
public class InviteJoinRecord {

    private ObjectId id; //记录id

    private @Indexed int inviteUserId;  //邀请者用户Id

    private @Indexed int joinUserId; //要加入的用户id

    private @Indexed String companyId; //公司id

    private @Indexed  String departmentId; //部门Id

    private long createTime; //创建时间

    private byte status; //状态 0 未确认 ， 1 已确认

    public InviteJoinRecord() { }


    public InviteJoinRecord(int inviteUserId, int joinUserId, String companyId, String departmentId) {
        this.inviteUserId = inviteUserId;
        this.joinUserId = joinUserId;
        this.companyId = companyId;
        this.departmentId = departmentId;
        this.createTime = DateUtil.currentTimeMilliSeconds();
    }
}
