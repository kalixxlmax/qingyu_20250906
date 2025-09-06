package com.basic.im.lable;

import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class UserLabelRepositoryImpl extends MongoRepository<UserLabel,ObjectId> implements UserLabelRepository{

	@Override
	public Class<UserLabel> getEntityClass() {
		return UserLabel.class;
	}

    @Override
    public Object addLabel(UserLabel userLabel) {
        return getDatastore().save(userLabel);
    }

    @Override
    public UserLabel getUserLabel(Integer userId, String labelId) {
        Query query = createQuery("userId",userId);
        addToQuery(query,"labelId",labelId);
        return findOne(query);
    }

    @Override
    public List<UserLabel> getUserLabels(Integer userId) {
        Query query=createQuery("userId",userId);
        return queryListsByQuery(query);
    }

    @Override
    public UserLabel queryUserLabel(Integer userId, String code) {
         Query query=createQuery("userId",userId);
         addToQuery(query,"code",code);
        return findOne(query);
    }
}
