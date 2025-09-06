package com.basic.im.company.entity;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * @Description:
 * @Author wxm
 * @Date 2021/4/22 15:52
 */
@ApiModel("Excel导入用户")
@Document(value = "UserImportExample")
public class UserImportExample {

    @ApiModelProperty("userID")
    private @Indexed int userId;

    @ApiModelProperty("姓名")
    private String name;

    @ApiModelProperty("英文名")
    private String englishName;

    @ApiModelProperty("电话号码")
    private String telephone;

    @ApiModelProperty("邮箱")
    private String email;

    @ApiModelProperty("部门名称")
    private @Indexed
    String departName;

    @ApiModelProperty("工号")// 新定义
    private @Indexed
    String employeeId;

    @ApiModelProperty("性别")
    private Byte sex; //0:男   1:女

    @ApiModelProperty("城市") //Excel可能是一个 中文名，先接收，然后和地区码比对。才确定是哪个城市
    private String city;

    @ApiModelProperty("直接主管") // 可以放 直接主管的手机号、邮箱、userID
    private String departmentDirector;

    @ApiModelProperty("人员类型") // 是否需要定义枚举类
    private Byte employeeType;

    @ApiModelProperty("是否部门负责人")
    private Byte isDepartmentDirector; // 1 是；0 否

    @ApiModelProperty("工位") // 新定义
    private String employeeStation;

    @ApiModelProperty("入职时间")
    private Long hiredate;

    @ApiModelProperty("手机号是否可见")
    private Byte showTelephone; //  -1 所有人不显示； 1 所有人显示；  2 所有好友显示； 3 手机联系人显示

    @ApiModelProperty("爱好")// 新定义
    private String hobby;

    public UserImportExample() {
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEnglishName() {
        return englishName;
    }

    public void setEnglishName(String englishName) {
        this.englishName = englishName;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getDepartName() {
        return departName;
    }

    public void setDepartName(String departName) {
        this.departName = departName;
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
    }

    public Byte getSex() {
        return sex;
    }

    public void setSex(Byte sex) {
        this.sex = sex;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getDepartmentDirector() {
        return departmentDirector;
    }

    public void setDepartmentDirector(String departmentDirector) {
        this.departmentDirector = departmentDirector;
    }

    public Byte getEmployeeType() {
        return employeeType;
    }

    public void setEmployeeType(Byte employeeType) {
        this.employeeType = employeeType;
    }

    public Byte getIsDepartmentDirector() {
        return isDepartmentDirector;
    }

    public void setIsDepartmentDirector(Byte isDepartmentDirector) {
        this.isDepartmentDirector = isDepartmentDirector;
    }

    public String getEmployeeStation() {
        return employeeStation;
    }

    public void setEmployeeStation(String employeeStation) {
        this.employeeStation = employeeStation;
    }

    public Long getHiredate() {
        return hiredate;
    }

    public void setHiredate(Long hiredate) {
        this.hiredate = hiredate;
    }

    public Byte getShowTelephone() {
        return showTelephone;
    }

    public void setShowTelephone(Byte showTelephone) {
        this.showTelephone = showTelephone;
    }

    public String getHobby() {
        return hobby;
    }

    public void setHobby(String hobby) {
        this.hobby = hobby;
    }
}
