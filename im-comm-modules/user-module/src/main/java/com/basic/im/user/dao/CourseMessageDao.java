package com.basic.im.user.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.CourseMessage;
import org.bson.types.ObjectId;

import java.util.List;

public interface CourseMessageDao extends IMongoDAO<CourseMessage,ObjectId> {

    void addCourseMessage(ObjectId courseMessageId,int userId,String courseId,String message,String messageId,String createTime);

    CourseMessage queryCourseMessageById(String courseMessageId);

    CourseMessage queryCourseMessageByIdOld(ObjectId courseMessageId);

    void deleteCourseMessage(CourseMessage courseMessage);

    List<CourseMessage> getCourseMessageList(String courseId,Integer userId);

}
