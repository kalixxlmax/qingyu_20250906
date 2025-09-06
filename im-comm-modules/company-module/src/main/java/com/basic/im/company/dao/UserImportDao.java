package com.basic.im.company.dao;

import com.basic.im.company.entity.UserImportExample;
import com.basic.mongodb.springdata.IBaseMongoRepository;
import org.bson.types.ObjectId;

/**
 * @Description:
 * @Author wxm
 * @Date 2021/4/23 10:39
 */
public interface UserImportDao extends IBaseMongoRepository<UserImportExample, ObjectId> {
    public  UserImportExample ImportUser(UserImportExample user);
}
