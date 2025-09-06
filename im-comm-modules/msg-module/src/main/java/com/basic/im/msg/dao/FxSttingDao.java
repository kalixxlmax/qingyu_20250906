package com.basic.im.msg.dao;

import com.basic.common.model.PageResult;
import com.basic.im.msg.entity.FxSetting;
import com.basic.im.repository.IMongoDAO;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

public interface FxSttingDao extends IMongoDAO<FxSetting, ObjectId> {

    void addMusicInfo(FxSetting musicInfo);

    void deleteMusicInfo(ObjectId id);

    FxSetting getMusicInfoById(ObjectId id);

    List<FxSetting> getMusicInfoList(int pageIndex, int pageSize);

    void updateMusicInfo(ObjectId id, Map<String, Object> map);

    PageResult<FxSetting> getMusicInfo(int pageIndex, int pageSize);

}
