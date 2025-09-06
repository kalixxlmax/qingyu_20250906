package com.basic.im.user.dao.impl;

import com.basic.im.repository.MongoRepository;
import com.basic.im.user.entity.Certification;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @author
 * @version V1.0
 * @Description: 身份认证持久化
 * @date 2020/3/23 15:34
 */
@Repository
public class VerifieRequestDaoImpl extends MongoRepository<Certification, ObjectId> {

    @Override
    public Class<Certification> getEntityClass() {
        return Certification.class;
    }

    public void saveCertified(Integer userId,int type,String certifiedAccount,String certifiedAddress,String vertifiedName,
        String backImageUrl,String frontImageUrl,String faceImageUrl,long startDate,long endDate){
        Certification certification = new Certification();
        certification.setUserId(userId);
        certification.setType(type);
        certification.setCertifiedAccount(certifiedAccount);
        certification.setCertifiedName(vertifiedName);
        certification.setCertifiedAddress(certifiedAddress);
        certification.setBackImageUrl(backImageUrl);
        certification.setFrontImageUrl(frontImageUrl);
        certification.setFaceImageUrl(faceImageUrl);
        certification.setStartDate(startDate);
        certification.setEndDate(endDate);
        getDatastore().save(certification);
    }

    public boolean getCertifiedInfo(int type, Integer userId) {
        Query query=createQuery("type",type);
        addToQuery(query,"userId",userId);
        return null != findOne(query);
    }

    public Certification queryCertifiedInfo(int type, Integer userId) {
        Query query=createQuery("type",type);
        addToQuery(query,"userId",userId);
        return findOne(query);
    }

    public Certification getCertifiedInfo(int type,String idcard,String name) {
        Query query=createQuery("type",type);
        addToQuery(query,"certifiedAccount",idcard);
        addToQuery(query,"certifiedName",name);
        return findOne(query);
    }

    public void deleteCertifiedInfo(int type, Integer userId){
        Query query=createQuery("type",type);
        addToQuery(query,"userId",userId);
        deleteByQuery(query);
    }


}
