package com.basic.im.invite.service.impl;

import com.basic.im.invite.dao.InviteDao;
import com.basic.im.invite.entity.Invite;
import com.basic.im.invite.service.InviteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author zhm
 * @version V1.0
 * @date 2019/11/14 19:41
 */
@Service
public class InviteServiceImpl implements InviteService {
    @Autowired
    private InviteDao inviteDao;

    // 添加关系
    @Override
    public void addInvite(Invite entity) {
        inviteDao.addInvite(entity);
    }

    // 查询上级
    @Override
    public List<Invite.Grade> queryGradeList(int userId) {
        return inviteDao.queryGradeList(userId);
    }

    // 查询下级
    @Override
    public List<Invite> queryInviteList(int userId) {
        return inviteDao.queryInviteList(userId);
    }
}
