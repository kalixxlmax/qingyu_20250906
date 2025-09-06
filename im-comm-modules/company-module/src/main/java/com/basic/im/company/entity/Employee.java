package com.basic.im.company.entity;

import com.basic.im.comm.constants.KConstants;
import com.basic.im.company.CompanyConstants;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;





@Data
@ApiModel("员工实体")
@Document(value = "employee")
public class Employee {

	@ApiModelProperty("员工id")
	private @Id ObjectId id; //员工id

	@ApiModelProperty("用户id,用于和用户表关联")
	private @Indexed int userId; //用户id,用于和用户表关联

	@ApiModelProperty("部门Id,表示员工所属部门  ")
	private @Indexed ObjectId departmentId;  //部门Id,表示员工所属部门

	@ApiModelProperty("公司id，表示员工所属公司")
	private @Indexed ObjectId companyId; //公司id，表示员工所属公司

	@ApiModelProperty("员工角色：0：普通员工     1：部门管理者(暂时没用)    2：公司管理员    3：公司创建者")
	private byte role; //员工角色：0：普通员工     1：部门管理者(暂时没用)    2：公司管理员    3：公司创建者

	@ApiModelProperty("职位（头衔），如：经理、总监等")
	private String position;  //职位（头衔），如：经理、总监等

	@ApiModelProperty("用户昵称，和用户表一致")
	private @Transient String nickname;  //用户昵称，和用户表一致

	private int retract; //缩进个数

	private int isStaff=0;//是否员工 1：是 0不是

	@Transient
	private int hiding = KConstants.LOSE; // 隐身模式，0关闭，1开启

	@ApiModelProperty("英文名")
	private String englishName;

	@ApiModelProperty("邮箱")
	private String email;

	@ApiModelProperty("工号")// 新定义
	private @Indexed
	String employeeId;

	@ApiModelProperty("人员类型") // 是否需要定义枚举类
	private Byte employeeType;

	@ApiModelProperty("是否部门负责人")
	private Byte isDepartmentDirector; // 1 是；0 否

	//工位==头衔

	@ApiModelProperty("入职时间")
	private Long hiredate;

	@ApiModelProperty("手机号是否可见")
	private Byte showTelephone; //  -1 所有人不显示； 1 所有人显示；  2 所有好友显示； 3 手机联系人显示

	@ApiModelProperty("爱好")// 新定义
	private String hobby;


	public Employee() { }


	public Employee(ObjectId departmentId, ObjectId companyId,int userId) {
		this.departmentId = departmentId;
		this.companyId =  companyId;
		this.role =  CompanyConstants.ROLE.COMMON_EMPLOYEE;
		this.userId = userId;

	}
}
