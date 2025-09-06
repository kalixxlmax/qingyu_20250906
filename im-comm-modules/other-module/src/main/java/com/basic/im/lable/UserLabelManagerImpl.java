package com.basic.im.lable;

import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserLabelManagerImpl extends MongoRepository<UserLabel, ObjectId> implements UserLabelManager{


    @Autowired
    UserLabelRepositoryImpl userLabelRepository;
	
	

    //添加群标识码
    @Override
    public Object addLabel(Integer userId,String labelId,String name,String logo,String code,long date) {

        UserLabel userLabel = new UserLabel();
        userLabel.setId(new ObjectId());
        userLabel.setUserId(userId);
        userLabel.setLabelId(labelId);
        userLabel.setCode(code);
        userLabel.setDate(date);
        userLabel.setName(name);
        userLabel.setLogo(logo);
        return userLabelRepository.addLabel(userLabel);
    }

    //获取用户群标识码列表
    @Override
    public List<UserLabel> getUserLabels(Integer userId) {
        return userLabelRepository.getUserLabels(userId);
    }

    @Override
    public UserLabel queryUserLabel(Integer userId, String labelId) {
        return userLabelRepository.getUserLabel(userId,labelId);
    }

    @Override
    public UserLabel queryUserLabelByCode(Integer userId, String code) {
        return  userLabelRepository.queryUserLabel(userId,code);
    }

    @Override
    public Class<UserLabel> getEntityClass() {
        return UserLabel.class;
    }
}
