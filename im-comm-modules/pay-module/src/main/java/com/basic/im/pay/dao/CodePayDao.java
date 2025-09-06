package com.basic.im.pay.dao;

import com.basic.im.pay.entity.CodePay;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface CodePayDao extends IMongoDAO<CodePay, ObjectId> {

    void addCodePay(CodePay codePay);

    double queryCodePayCount(int userId);

    double queryToDayCodePayCount(int userId);
}
