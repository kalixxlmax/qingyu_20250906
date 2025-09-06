package com.basic.im.company.controller;

import com.basic.im.comm.utils.StringUtil;
import com.basic.im.company.CompanyConstants;
import com.basic.im.company.dao.CompanyDao;
import com.basic.im.company.dao.DepartmentDao;
import com.basic.im.company.dao.EmployeeDao;
import com.basic.im.company.entity.Company;
import com.basic.im.company.entity.Department;
import com.basic.im.company.entity.Employee;
import com.basic.im.company.entity.UserImportExample;
import com.basic.im.company.service.CompanyManager;
import com.basic.im.company.service.UserImportManager;
import com.basic.im.i18n.LocaleMessageUtils;
import com.basic.im.user.dao.UserCoreDao;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.ExcelUtil;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.DateUtil;
import com.basic.utils.Md5Util;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;



/**
 * @Description:
 * @Author Administrator
 * @Date 2021/4/21 18:37
 */

@ApiIgnore
@RestController
@RequestMapping(value = "/console/excel")
public class ExcelController {
    protected Logger logger= LoggerFactory.getLogger(ExcelController.class );

    @Autowired
    private UserImportManager userImportManager;

    @Autowired(required = false)
    private CompanyManager companyManager;

    @Autowired(required = false)
    private CompanyDao companyDao;

    @Autowired(required = false)
    private DepartmentDao departmentDao;

    // @Autowired(required = false)
    // private UserManager userManager;

    // @Autowired(required = false)
    // private UserDao userDao;


    @Autowired(required = false)
    private UserCoreService userCoreService;

    @Autowired(required = false)
    private UserCoreDao userCoreDao;

    @Autowired(required = false)
    private EmployeeDao employeeDao;


    /**
     * 读取excel文件中的用户信息，保存在数据库中
     * @param file
     * @param req
     * @param resp
     * @return
     */
    @RequestMapping("/importUserExcelData")
    public JSONMessage importUserExcelData(@RequestParam(value = "file") MultipartFile file, HttpServletRequest req, HttpServletResponse resp){
        // Map<String, Object> param = new HashMap<String, Object>();
        List<UserImportExample> userImportExampleList = new ArrayList<UserImportExample>();
        // 读取excel文件
        try {
            List<String[]> userList = ExcelUtil.readExcel(file);
            // logger.info("userList:"+userList.size());
            for(int i = 0;i<userList.size();i++){
                String[] users = userList.get(i);

                // logger.info("users:"+users.length);
                //
                // for (int j = 0; j < users.length; j++) {
                //     logger.info("user:"+users[j]);
                // }

                UserImportExample user = new UserImportExample();

                // user.setUserId(Integer.valueOf(users[0]));
                // user.setUserId(userCoreService.createUserId());
                user.setName(users[1]);
                user.setEnglishName(users[2]);
                user.setTelephone(users[3]);
                user.setEmail(users[4]);
                user.setDepartName(users[5]);
                user.setEmployeeId(users[6]);

                // user.setSex(Byte.valueOf(users[7]));
                int sex=  users[7] .equals("男") ? 0 : 1;
                user.setSex(Byte.valueOf(sex+""));
                // System.out.println("setSex:"+users[7]);

                user.setCity(users[8]);
                user.setDepartmentDirector(users[9]);

                // user.setEmployeeType(Byte.valueOf(users[10]));
                int employeeType=users[10] .equals("正式")  ? 1 : 0;
                user.setEmployeeType(Byte.valueOf(employeeType+""));
                // System.out.println("employeeType:"+users[10]);

                // user.setIsDepartmentDirector(Byte.valueOf(users[11]));
                int isDepartmentDirector=users[11].equals("是")  ? 1 : 0;
                user.setIsDepartmentDirector(Byte.valueOf(isDepartmentDirector+""));
                // System.out.println("isDepartmentDirector:"+users[11]);


                user.setEmployeeStation(users[12]);




                // Long millisecond = Instant.now().toEpochMilli();  // 精确到毫秒
                // Long second = Instant.now().getEpochSecond();// 精确到秒

                // user.setHiredate(Long.valueOf(users[13]));
                // user.setHiredate(second);

                DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
                String str = users[13];
                Date date = format1.parse(str);
                // System.out.println("date:"+date);

                user.setHiredate(date.getTime());



                // user.setShowTelephone(Byte.valueOf(users[14]));
                int showTelephone=users[14] .equals("是")  ? 1 : 0;
                user.setShowTelephone(Byte.valueOf(showTelephone+""));
                // System.out.println("showTelephone:"+users[14]);

                user.setHobby(users[15]);

                // user.setUserName(users[0]);
                userImportExampleList.add(user);
            }
        } catch (IOException | ParseException e) {
            // logger.info("读取excel文件失败", e);
            return JSONMessage.success("读取excel文件失败");
        }
        // param.put("allUsers", allUsers);

        // 存到数据库当中
        // for (int i = 0; i < allUsers.size(); i++) {
        //     userImportManager.ImportUser(allUsers.get(i));
        // }


        try {
            // 把excel的数据导入到数据库中
            importUserExcelDataToDB(userImportExampleList);
            return JSONMessage.success("数据导入成功",null);
        } catch (Exception e) {
            e.printStackTrace();
            return JSONMessage.failure("数据导入失败");
        }


    }

    private void importUserExcelDataToDB(List<UserImportExample> userImportExampleList) {
        // 第一步 存到员工表
        // List<Employee> employeeList = new ArrayList<Employee>();
        for (int i = 0; i < userImportExampleList.size(); i++) {
            UserImportExample userTemp=userImportExampleList.get(i);

            Employee employee=new Employee();


            // 第三步 如果员工没有注册账号，则注册一下（存到用户表）

            // 因为员工表里面没有手机号，单单根据姓名来判断是否重复，不合理。
            // 所以，只能是 导入的员工数据 先到用户表查手机号，如果重复，则返回userID。
            // 再用userID去查询员工表，如果查询为空，则证明员工表里面没有该员工，可以插入。否则是重复，不给插入。

            //TODO 有些手机号是加了地区码的，例如：8617302688898.需要去掉前面两个字符，再查数据库

            String areaCode = "86";
            User findUser=userCoreDao.getUser(areaCode+userTemp.getTelephone());
          if(findUser==null){
              // 员工不存在用户表
              User user=new User();
              int userIdTemp=userCoreService.createUserId();
              user.setUserId(userIdTemp);
              user.setUsername(userTemp.getName());
              // user.setPassword(Md5Util.md5Hex(user.getTelephone()));

              // 地区码+手机号 再 MD5加密得到UserKey

              user.setUserKey(Md5Util.md5Hex(areaCode+userTemp.getTelephone()));

              user.setAccount(userIdTemp + StringUtil.randomCode());
              user.setEncryAccount(Md5Util.md5Hex(userIdTemp + StringUtil.randomCode()));

              // 设置默认密码为123456
              // user.setPassword(Md5Util.md5Hex(user.getPassword()));
              user.setPassword(Md5Util.md5Hex("123456"));

              user.setTelephone(areaCode+userTemp.getTelephone());
              user.setPhone(userTemp.getTelephone());
              user.setName(userTemp.getName());
              user.setSex(Integer.valueOf(userTemp.getSex()));

              user.setNickname(userTemp.getName());


              // 需要查询城市名，然后找到对应的城市ID
              user.setCountryId(0);
              user.setProvinceId(0);
              user.setCityId(0);

              user.setAreaCode("86");

              // Long millisecond = Instant.now().toEpochMilli();  // 精确到毫秒
              Long second = Instant.now().getEpochSecond();// 精确到秒

              user.setCreateTime(second);
              user.setModifyTime(second);

              user.setBirthday(second);
              user.setDescription(userTemp.getName());
              user.setAreaId(0);
              user.setLevel(0);
              user.setVip(0);

              User.UserSettings settings = new User.UserSettings();
              user.setSettings(settings);

              user.setLoc(new User.Loc(10.0,10.0));

              // System.out.println("用户不存在，需要注册");

              userCoreDao.addUser(user);

              userTemp.setUserId(user.getUserId());
              addEmployee(userTemp, employee);
          }
          else {
              // 员工存在用户表
              // System.out.println("findUser.getUserId():"+findUser.getUserId());

              // 看一下员工是否在员工表里面,不存在，则插入。
              List<Employee> findEmployeeList=employeeDao.findByUserId(findUser.getUserId());
              if(findEmployeeList.size()==0){

                  userTemp.setUserId(findUser.getUserId());
                  addEmployee(userTemp, employee);
              }

          }

        }
    }

    /**
     * 添加员工到员工表，并更新+ 部门表的直接主管的字段
     */
    private void addEmployee(UserImportExample userTemp, Employee employee) {
        employee.setEnglishName(userTemp.getEnglishName());
        employee.setEmail(userTemp.getEmail());
        employee.setEmployeeId(userTemp.getEmployeeId());
        employee.setEmployeeType(userTemp.getEmployeeType());
        employee.setIsDepartmentDirector(userTemp.getIsDepartmentDirector());
        employee.setHiredate(userTemp.getHiredate());
        employee.setShowTelephone(userTemp.getShowTelephone());
        employee.setHobby(userTemp.getHobby());

        employee.setUserId(userTemp.getUserId());
        // employee.setUserId(userCoreService.createUserId());


        String companyDepartmentName=userTemp.getDepartName();
        String[] nameString=companyDepartmentName.split("/");
        String companyName=nameString[0];
        ;
        String departmentName=nameString[1];

        //需要到公司表查询公司名称，如果存在则保存公司ID
        Company company=companyDao.findOneByName(companyName);
        if(company!=null){
            employee.setCompanyId(company.getId());
            // System.out.println("company:"+company.getId());
        }

        //需要到部门表查询部门名称，如果存在则保存部门ID

        Department department=departmentDao.findOneByName(company.getId(),departmentName);
        if(department!=null){
            employee.setDepartmentId(department.getId());
            // System.out.println("department:"+department.getId());

            // 第二步 存到部门表

            // 同时更新 直接主管的信息
            department.setDepartmentDirector(userTemp.getDepartmentDirector());
            // 部门人数+1
            department.setEmpNum(department.getEmpNum()+1);
            departmentDao.modifyDepartment(department);

            // 公司人数+1
            company.setEmpNum(company.getEmpNum()+1);
            companyDao.modifyCompany(company);
            // System.out.println("同时更新 直接主管的信息");
        }

        // 工位==头衔
        employee.setPosition(userTemp.getEmployeeStation());

        // employeeList.add(employee);
        companyManager.importEmployee(employee);
    }


    /**
     * Excel导入部门
     */
    @RequestMapping("/importDepartmentExcelData")
    public JSONMessage importDepartmentExcelData(@RequestParam(value = "file") MultipartFile file, HttpServletRequest req, HttpServletResponse resp){
        List<Department> departmentImportList = new ArrayList<Department>();
        // 读取excel文件
        try {
            List<String[]> departmentList = ExcelUtil.readExcel(file);
            // logger.info("departmentList:"+departmentList.size());
            for(int i = 0;i<departmentList.size();i++) {
                String[] departmentString = departmentList.get(i);

                // for (int j = 0; j < departmentString.length; j++) {
                //     System.out.println("departmentString:"+departmentString[j]);
                // }

                // companyDepartmentName 例如：字节有限公司/研发部
                // 需要分割 出公司名和部门名

                String companyDepartmentName=departmentString[1];
                String[] nameString=companyDepartmentName.split("/");
                String companyName=nameString[0];
                String departmentName=nameString[1];
                // System.out.println("companyName:"+companyName);
                // System.out.println("departmentName:"+departmentName);

                //需要到公司表查询公司名称，如果存在则 公司ID 后面要用
                Company findCompany=companyDao.findOneByName(companyName);
                if(findCompany==null){
                    // TODO 没有公司，则创建公司。
                    // createCompanyAndDepartment(companyName, departmentName);
                }
                // 已经有了公司。则导入某个部门
                else {
                    // 当前登录的账号的用户id
                    int createUserId=100002;
                    // TODO token被忽略了，所以userID解析不出来。为null值。暂时用固定的userID先
                    // int createUserId=ReqUtil.getUserId();;


                    // 先查询这个部门是否存在
                    Department findDepartment=departmentDao.findOneByName(findCompany.getId(),departmentName);

                    // 部门不存在，则创建
                    if(findDepartment==null){
                        createDepartment(findCompany, departmentName,createUserId);
                    }

                }


            }

            return JSONMessage.success("数据导入成功",null);
        } catch (IOException e) {
            e.printStackTrace();
            return JSONMessage.failure("数据导入失败");
        }
    }

    /**
     * 创建一家公司，并创建一个部门
     */
    private void createCompanyAndDepartment(String companyName, String departmentName) {
        // 当前登录的账号的用户id
        int createUserId=100002;
        // TODO token被忽略了，所以userID解析不出来。为null值。暂时用固定的userID先
        // int createUserId=ReqUtil.getUserId();;

        ObjectId rootDpartId = new ObjectId();

        //添加公司记录
        // TODO Company的rootDpartId 设计得有些问题，暂时不管。
        Company company =  companyDao.addCompany(companyName, createUserId, rootDpartId);
        Locale requestLocale = LocaleMessageUtils.getRequestLocale();

        //在部门表：给该公司默认添加一条根部门记录
        Department department = new Department();
        department.setCompanyId(company.getId());
        department.setParentId(null);  //根部门的ParentId 为null
        department.setDepartName(companyName);
        department.setCreateUserId(createUserId); //根部门的创建者即公司的创建者
        department.setCreateTime(DateUtil.currentTimeSeconds());
        department.setEmpNum(0);
        department.setType(1);  //1:根部门

        rootDpartId = departmentDao.addDepartment(department); //添加根部门记录
        List<ObjectId> rootList = new ArrayList<ObjectId>();
        rootList.add(rootDpartId);

        //在部门表：给该公司创建一个部门
        Department personDepart = new Department();
        personDepart.setCompanyId(company.getId());
        personDepart.setParentId(rootDpartId);  //ParentId 为根部门的id
        // personDepart.setDepartName(LocaleMessageUtils.getMessage("personalDepartment",requestLocale));
        personDepart.setDepartName(departmentName);
        personDepart.setCreateUserId(createUserId); //创建者即公司的创建者
        personDepart.setCreateTime(DateUtil.currentTimeSeconds());
        personDepart.setEmpNum(1);
        ObjectId personDepartId = departmentDao.addDepartment(personDepart); //添加部门记录


        //在员工表：给创建者添加员工记录，将其置于 最前面的部门中 （TODO 默认是置于 人事部 中）
        Employee employee = new Employee();
        employee.setDepartmentId(personDepartId);
        employee.setRole(CompanyConstants.ROLE.COMPANY_CREATER);   //3：公司创建者(超管)
        employee.setUserId(createUserId);
        employee.setCompanyId(company.getId());
        employee.setPosition(LocaleMessageUtils.getMessage("creator",requestLocale));
        employeeDao.addEmployee(employee);

        company.setRootDpartId(rootList);
        company.setCreateTime(DateUtil.currentTimeSeconds());
        //将根部门id存入公司记录
        companyDao.modifyCompany(company);
    }

    /**
     * 创建一家公司
     */
    private void createCompany(String companyName,  int createUserId) {
        ObjectId rootDpartId = new ObjectId();
        //添加公司记录
        // TODO Company的rootDpartId 设计得有些问题，暂时不管。
        Company company =  companyDao.addCompany(companyName, createUserId, rootDpartId);
    }


    /**
     * 创建一个部门
     */
    private void createDepartment(Company company, String departmentName,int createUserId) {

        // Company company=companyDao.findOneByName(companyName);
        Locale requestLocale = LocaleMessageUtils.getRequestLocale();

        //在部门表：给该公司创建一个部门
        Department personDepart = new Department();
        personDepart.setCompanyId(company.getId());

        // personDepart.setParentId(rootDpartId);  //ParentId 为根部门的id
        personDepart.setParentId(company.getId());  //ParentId 为根部门的id。即公司的ID

        // personDepart.setDepartName(LocaleMessageUtils.getMessage("personalDepartment",requestLocale));
        personDepart.setDepartName(departmentName);

        personDepart.setCreateUserId(createUserId); //创建者即公司的创建者
        personDepart.setCreateTime(DateUtil.currentTimeSeconds());
        personDepart.setEmpNum(1);
        ObjectId personDepartId = departmentDao.addDepartment(personDepart); //添加部门记录


        //在员工表：给创建者添加员工记录，将其置于 最前面的部门中 （TODO 默认是置于 人事部 中）
        Employee employee = new Employee();
        employee.setDepartmentId(personDepartId);
        employee.setRole(CompanyConstants.ROLE.COMPANY_CREATER);   //3：公司创建者(超管)
        employee.setUserId(createUserId);
        employee.setCompanyId(company.getId());
        employee.setPosition(LocaleMessageUtils.getMessage("creator",requestLocale));
        employeeDao.addEmployee(employee);

        // company.setRootDpartId(rootList);
        // company.setCreateTime(DateUtil.currentTimeSeconds());
        //将根部门id存入公司记录
        // companyDao.modifyCompany(company);
    }

}
