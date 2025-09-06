package com.basic.im.user.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.UserSign;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

public interface ExtendManagerDao extends IMongoDAO<UserSign, ObjectId> {

    UserSign sava(UserSign userSign);

    List<UserSign> getUserSignByUserIdAndSignDate(Integer userId, Date yesDate);

    List<UserSign> findUserSignByMouth(Integer userId, Date parse, Date parse1);
}
