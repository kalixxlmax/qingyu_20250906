package com.basic.im.user.dao;

import com.basic.im.repository.IMongoDAO;
import com.basic.im.user.entity.Course;
import org.bson.types.ObjectId;

import java.util.List;

public interface CourseDao extends IMongoDAO<Course,ObjectId> {

    void addCourse(Course course);

    Course getCourseById(ObjectId courseId);

    Course getCourse(ObjectId courseId,Integer userId);

    List<Course> getCourseListByUserId(Integer userId);

    void updateCourse(ObjectId courseId,List<String> messageIds,long updateTime,String courseName);

    void deleteCourse(ObjectId courseId,Integer userId);


}
