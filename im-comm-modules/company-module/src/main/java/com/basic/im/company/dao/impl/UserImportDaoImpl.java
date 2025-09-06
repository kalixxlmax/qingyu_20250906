package com.basic.im.company.dao.impl;

import com.basic.im.company.dao.EmployeeDao;
import com.basic.im.company.dao.UserImportDao;
import com.basic.im.company.entity.Employee;
import com.basic.im.company.entity.UserImportExample;
import com.basic.mongodb.springdata.BaseMongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @Description:
 * @Author wxm
 * @Date 2021/4/23 10:39
 */
@Repository
public class UserImportDaoImpl extends BaseMongoRepository<UserImportExample, ObjectId> implements UserImportDao {

    @Override
    public UserImportExample ImportUser(UserImportExample user) {
        // Query query = createQuery("departmentId",user);
        // queryListsByQuery(query);

        UserImportExample temp=getDatastore().save(user);//存入员工数据

        return temp;
    }
}
