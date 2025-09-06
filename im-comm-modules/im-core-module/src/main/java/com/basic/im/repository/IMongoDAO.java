package com.basic.im.repository;

import com.basic.mongodb.springdata.IBaseMongoRepository;

import java.io.Serializable;

public interface IMongoDAO<T,ID extends Serializable> extends IBaseMongoRepository<T,ID> {


}
