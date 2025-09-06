package com.basic.im.lable;

import java.util.List;

import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import static com.basic.im.utils.SKBeanUtils.getDatastore;


@Service
public class LabelRepositoryImpl extends MongoRepository<Label,ObjectId> implements LabelRepository{

	@Override
	public Class<Label> getEntityClass() {
		return Label.class;
	}

    @Override
    public Object createLabel(Label label) {
       return getDatastore().save(label);
    }

    @Override
    public List<Label> getLabelList(Integer userId) {
        return getEntityListsByKey("userId",userId);
    }

    @Override
    public Label getLabel(ObjectId labelId) {
       return get(labelId);
    }

    @Override
    public Label getLabelByCode(String code) {
        return findOne("code",code);
    }

    @Override
    public Label updateLabel(Label label) {
        Query query = createQuery("code",label.getCode());
        if(findOne(query) == null)
            return  null;

        Update ops = createUpdate();

        if(null != label.getName())
            ops.set("name",label.getName());
        if(null != label.getLogo())
            ops.set("logo",label.getLogo());
        if(null != label.getMark())
            ops.set("mark",label.getMark());

        return getDatastore().findAndModify(query, ops,getEntityClass());
    }

    @Override
    public Object saveLabel(ObjectId id, String logo, String name) {
        Query query =createQuery(id);

        Update ops = createUpdate();

        if(null != name)
            ops.set("name",name);
        if(null != logo)
            ops.set("logo",logo);

        return getDatastore().findAndModify(query, ops,getEntityClass());
    }

    @Override
    public Label queryLabelByName(String name) {
        return  findOne("name",name);
    }

}
