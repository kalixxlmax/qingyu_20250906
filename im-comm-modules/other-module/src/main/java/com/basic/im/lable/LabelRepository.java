package com.basic.im.lable;

import java.util.List;

import org.bson.types.ObjectId;

/**
* 
* @author lidaye
* @date 2018年7月24日 
*/
public interface LabelRepository {

	/**
	* 
	* @param @param label
	* @param @return    参数
	*/
	Object createLabel(Label label);

	/**
	* 
	* @param @param userId
	* @param @return    参数
	*/
	List<Label> getLabelList(Integer userId);

	/**
	* 
	* @param @param labelId
	* @param @return    参数
	*/
	Label getLabel(ObjectId labelId);

	/**
	* 
	* @param @param code
	* @param @return    参数
	*/
	Label getLabelByCode(String code);

	/**
	* 
	* @param @param label
	* @param @return    参数
	*/
	Label updateLabel(Label label);

	/**
	* 
	* @param @param id
	* @param @param logo
	* @param @param name
	* @param @return    参数
	*/
	Object saveLabel(ObjectId id, String logo, String name);

	/**
	* 
	* @param @param name
	* @param @return    参数
	*/
	Label queryLabelByName(String name);

}

