package com.basic.im.admin.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.KeywordDAO;
import com.basic.im.admin.entity.KeyWord;
import com.basic.im.admin.entity.KeywordDenyRecord;
import com.basic.im.admin.service.KeywordManager;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.message.IMessageService;
import com.basic.im.message.MessageType;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KeywordManagerImpl implements KeywordManager {
	
	@Autowired
	private KeywordDAO keywordDAO;
	
	@Autowired
	private UserCoreService userCoreService;

	@Autowired
	private  RoomManagerImplForIM roomManager;

	@Autowired
	private IMessageService messageService;
	
	@Override
	public void addKeyword(String word,String id) {
		KeyWord keyword = null;
		if (StringUtil.isEmpty(id)) {
			keyword = new KeyWord();
			keyword.setWord(word);
			keyword.setCreateTime(DateUtil.currentTimeSeconds());
			keywordDAO.saveKeyword(keyword);
		}else{
			keywordDAO.updateKeyword(word, new ObjectId(id));
		}
		sendRefreshSysConfig();
	}

	@Override
	public void deleteKeyword(ObjectId id) {
		keywordDAO.deleteKeyword(id);
		sendRefreshSysConfig();
	}

	@Override
	public void sendRefreshSysConfig() {
		if (1 == SKBeanUtils.getSystemConfig().getIsKeyWord()) {
			MessageBean messageBean = new MessageBean();
			messageBean.setContent("1");
			messageBean.setType(MessageType.REFRESH_SYSCONFIG);
			messageBean.setFromUserId("10005");
			messageBean.setToUserId("10005");
			messageService.send(messageBean);
		}
	}

	@Override
	public List<KeyWord> queryKeywordList(String word, int pageIndex, int pageSize) {
		return keywordDAO.queryKeywordList(word, pageIndex, pageSize);
	}

	@Override
	public List<KeywordDenyRecord> queryKeywordDenyRecordList(Integer userId, String toUserId, int pageIndex, int pageSize,
													int type, String content) {
		List<KeywordDenyRecord> data = keywordDAO.queryMsgInerceptList(userId, toUserId, pageIndex, pageSize, type, content);
		
		for(KeywordDenyRecord keywordDenyRecord : data){
			keywordDenyRecord.setFromUserName(userCoreService.getNickName(Integer.valueOf(keywordDenyRecord.getFromUserId())));
			if(1 == keywordDenyRecord.getChatType() ){
				keywordDenyRecord.setToUserName(userCoreService.getNickName(Integer.valueOf(keywordDenyRecord.getToUserId())));
			}else{
				keywordDenyRecord.setToUserName(roomManager.getRoomName(keywordDenyRecord.getRoomJid()));
			}
		}
		return data;
	}

	@Override
	public void deleteMsgIntercept(ObjectId id) {
		keywordDAO.deleteMsgIntercept(id);
	}

	@Override
	public PageResult<KeywordDenyRecord> webQueryKeywordDenyRecordList(Integer userId, String toUserId, int pageIndex, int pageSize,
															int type, String content) {
		PageResult<KeywordDenyRecord> data = keywordDAO.webQueryMsgInterceptList(userId, toUserId, pageIndex, pageSize, type, content);

		for(KeywordDenyRecord keywordDenyRecord : data.getData()){
			keywordDenyRecord.setFromUserName(userCoreService.getNickName(Integer.valueOf(keywordDenyRecord.getFromUserId())));
			if(1 == keywordDenyRecord.getChatType() ){
				keywordDenyRecord.setToUserName(userCoreService.getNickName(Integer.valueOf(keywordDenyRecord.getToUserId())));
			}else{
				keywordDenyRecord.setToUserName(roomManager.getRoomName(keywordDenyRecord.getRoomJid()));
			}
		}
		return data;
	}
	
}
