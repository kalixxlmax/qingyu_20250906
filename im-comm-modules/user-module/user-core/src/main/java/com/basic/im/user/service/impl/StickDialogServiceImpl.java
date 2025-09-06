package com.basic.im.user.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.user.dao.StickDialogDao;
import com.basic.im.entity.StickDialog;
import com.basic.im.user.entity.User;
import com.basic.im.user.event.DeleteUserEvent;
import com.basic.im.user.service.StickDialogService;
import com.basic.mongodb.wrapper.QueryWrapper;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class StickDialogServiceImpl implements StickDialogService {

    @Autowired
    private StickDialogDao stickDialogDao;

    @Override
    public void add(StickDialog stickDialog) {
        stickDialogDao.save(stickDialog);
    }

    @Override
    public void delete(Integer userId,String jid) {
        VerifyUtil.execute(ObjectUtil.isAllNotEmpty(userId,jid),()-> stickDialogDao.deleteByQuery(QueryWrapper.query(StickDialog::getUserId, userId).eq(StickDialog::getJid,jid).build()));
    }

    @Override
    public List<StickDialog> findListByUserId(Integer userId) {
        return stickDialogDao.queryListsByQuery(QueryWrapper.query(StickDialog::getUserId,userId).build());
    }

    @Override
    public void destroyUserRecord(Integer userId) {
         stickDialogDao.deleteByQuery(QueryWrapper.query(StickDialog::getUserId,userId).build());


        stickDialogDao.deleteByQuery(QueryWrapper.query(StickDialog::getJid,userId+"").build());
    }


    @EventListener
    public void handlerDeleteUserEvent(DeleteUserEvent event) {
        // 退出用户加入的群聊、解散创建的群组
        try {
           destroyUserRecord(event.getUserId());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
