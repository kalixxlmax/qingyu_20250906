package com.basic.im.user.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.StickDialogDao;
import com.basic.im.entity.StickDialog;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Repository;

@Repository
public class StickDialogDaoImpl extends MongoRepository<StickDialog, ObjectId> implements StickDialogDao {
    @Override
    public Class<StickDialog> getEntityClass() {
        return StickDialog.class;
    }



}
