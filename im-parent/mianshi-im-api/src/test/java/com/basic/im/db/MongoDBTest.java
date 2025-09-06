package com.basic.im.db;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.company.dao.DepartmentDao;
import com.basic.im.company.entity.Employee;
import com.basic.im.utils.MongoUtil;
import com.basic.utils.Md5Util;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/*@RunWith(SpringRunner.class)
@SpringBootTest(classes= Application.class)*/
public class MongoDBTest {

	//@Autowired
	private DepartmentDao departmentDao;


	@Test
	public void testAddressDB() {
		//SKBeanUtils.getCompanyManager().createCompany("广州科技有限公司", 54123);
		Employee employee = new Employee();
		employee.setUserId(10000050);
		employee.setNickname("变形金刚");
		employee.setDepartmentId(new ObjectId("5d8a0473514f773e080a6ec8"));
		employee.setCompanyId(new ObjectId("5d8a0473514f773e080a6ec4"));
		employee.setRole((byte) 0);
		employee.setPosition("普通员工");
		System.out.println(Md5Util.md5Hex("8610000"));

//		SKBeanUtils.getEmployeeDaoManager().addEmployee(employee);
		/*Department Customer = new Department();
		Customer.setCompanyId(new ObjectId("5d8a0473514f773e080a6ec4"));
		Customer.setParentId(new ObjectId("5d905faf514f77372436a294"));  //ParentId 为根部门的id
		Customer.setDepartName("研发部");
		Customer.setCreateUserId(54123); //创建者即公司的创建者
		Customer.setCreateTime(DateUtil.currentTimeSeconds());
		Customer.setEmpNum(0);
		Customer.setType(1);
		departmentDao.addDepartment(Customer); //添加部门记录*/
	}
	
	
	@Test
	public void testRoomFenBiao() {
		System.out.println(MongoUtil.tranKeyWord("IM-比高 (1+6)"));
	}
	@Test
	public void testDBObject() {
		DBObject dbObject=new BasicDBObject("aa",111);
//		MongoCollection collection = SKBeanUtils.getLocalSpringBeanManager().getMongoClient().getDatabase("test1").getCollection("test",DBObject.class);
//		collection.insertOne(dbObject);
	}
	


	@Test
	public void getUserMoney(){
//		Double monet = yopWalletBillService.getUserYopRedPacktMoney(10002382,1, "2019-11-18 00:00:00" ,"2019-11-18 23:20:47");
//		System.out.println("用户充值金额 "+monet);
	}


	@Test
	public void getUserFirstRecharge(){

		System.out.println(MongoUtil.tranKeyWord("^"));
	}
}
