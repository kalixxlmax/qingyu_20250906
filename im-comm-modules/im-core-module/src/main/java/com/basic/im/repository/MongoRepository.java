package com.basic.im.repository;

import com.basic.im.comm.constants.KConstants;
import com.basic.mongodb.springdata.BaseMongoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.repository.NoRepositoryBean;

import java.io.Serializable;
@NoRepositoryBean
public abstract class MongoRepository<T,ID extends Serializable> extends BaseMongoRepository<T,ID> implements IMongoDAO<T,ID> {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());





	public abstract Class<T> getEntityClass();



	/**
	 * 根据 用户 Id 即 取余 值  获取 实体表名 
	 * @param userId 
	 * @param remainder  取余值
	 * @return
	 */
	public String getCollectionName(int userId,int remainder) {
		String collectionName=null;

		if(userId> KConstants.MIN_USERID) {
			remainder=userId/remainder;
		}
		return String.valueOf(remainder);
	}



}
