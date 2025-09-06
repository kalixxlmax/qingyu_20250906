package com.basic.im.invite.service;

import com.basic.im.invite.entity.Invite;

import java.util.List;

public interface InviteService {

    void addInvite(Invite entity);

    List<Invite.Grade> queryGradeList(int userId);

    List<Invite> queryInviteList(int userId);
}
