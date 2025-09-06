package com.basic.im.user.service;

import java.util.List;

public interface RoleManager {
    List<Integer> getUserRoles(Integer toUserId);
}
