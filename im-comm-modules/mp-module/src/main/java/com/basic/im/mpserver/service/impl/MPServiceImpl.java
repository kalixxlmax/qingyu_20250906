package com.basic.im.mpserver.service.impl;

import com.basic.common.model.PageVO;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.dao.TigaseMsgDao;
import com.basic.im.mpserver.service.MPService;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MPServiceImpl implements MPService {

	@Autowired
	private TigaseMsgDao tigaseMsgDao;


	@Autowired
	private IMessageRepository messageRepository;




	Document getLastBody(int sender, int receiver) {
		return tigaseMsgDao.getLastBody(sender,receiver);
	}

	/**
	 *  消息分组分页查询
	 */
	@Override
	public PageVO getMsgList(int userId, int pageIndex, int pageSize) {
		long total =0;
		return new PageVO(tigaseMsgDao.getMsgList(userId,pageIndex,pageSize),total,pageIndex,pageSize);
	}

	@Override
	public Object getMsgList(int sender, int receiver, int pageIndex, int pageSize) {
		return tigaseMsgDao.getMsgList(sender,receiver,pageIndex,pageSize);
	}


	public List<Document> queryLastChatList(int userId ,long startTime, int pageSize) {
		return tigaseMsgDao.queryLastChatList(userId,startTime,pageSize);
	}
}
