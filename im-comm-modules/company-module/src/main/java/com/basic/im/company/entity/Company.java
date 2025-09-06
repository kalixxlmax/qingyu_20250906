package com.basic.im.company.entity;

import com.basic.im.utils.SKBeanUtils;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Data
@Document(value = "company")
public class Company {

    //公司id
	private @Id ObjectId id;

	//公司名称
	private @Indexed String companyName;

	//创建者的用户id
	private @Indexed int createUserId;

	//删除者id,默认0   注：当用户执行删除公司操作后，将userId存入，隐藏相关信息。
	private @Indexed int deleteUserId;

	//根部门Id,可能有多个
	private @Indexed List<ObjectId> rootDpartId;


	//创建时间
	private @Indexed long createTime;

	//删除时间
	private long deleteTime;

	//公司公告（通知）
	private @Indexed String noticeContent;

	//公告时间
	private long noticeTime;

	//公司员工总数
	private @Indexed int empNum;

	//类型值     5:默认加入的公司
	private @Indexed int type = 0;

	//是否审核通过  1 是 0 否
	private @Indexed byte isChecked;

	//临时数据 当前用户是否存在公司中 1 是 0 否
	private @Transient byte currentUserIsExist;

	private @Transient byte userJoinCompanyIsNeedManagerConfirm; //用户主动加入公司是否需要创建者确认 0 否 1 是

	private @Transient byte inviteJoinCompanyIsNeedUserConfirm; //邀请加入公司是否需要用户确认 0 否 1 是


	//公司的部门列表
	@Transient
	private List<Department> departments;

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	public List<Department> getDepartments() {
		return departments;
	}
	
	public void setDepartments(List<Department> departments) {
		this.departments = departments;
	}


	public byte getUserJoinCompanyIsNeedManagerConfirm() {
		return SKBeanUtils.getImCoreService().getConfig().getUserJoinCompanyIsNeedManagerConfirm();
	}

	public byte getInviteJoinCompanyIsNeedUserConfirm() {
		return SKBeanUtils.getImCoreService().getConfig().getInviteJoinCompanyIsNeedUserConfirm();
	}
}