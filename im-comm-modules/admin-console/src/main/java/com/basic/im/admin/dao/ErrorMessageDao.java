package com.basic.im.admin.dao;


import com.basic.common.model.PageResult;
import com.basic.im.model.ErrorMessage;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

public interface ErrorMessageDao extends IMongoDAO<ErrorMessage, ObjectId> {

    void addErrorMessage(ErrorMessage errorMessage);

    PageResult getErrorMessageList(String keyword, int pageIndex, int pageSize);

    void deleteErrorMessage(String code);

    ErrorMessage getErrorMessage(String code);

    ErrorMessage getErrorMessage(ObjectId id);

    ErrorMessage updateErrorMessage(ObjectId id, ErrorMessage errorMessage);
}
