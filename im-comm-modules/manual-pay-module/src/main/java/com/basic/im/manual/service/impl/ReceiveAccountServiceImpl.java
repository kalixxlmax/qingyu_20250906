package com.basic.im.manual.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.manual.dao.ReceiveAccountDao;
import com.basic.im.manual.entity.ReceiveAccount;
import com.basic.im.manual.service.ReceiveAccountService;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/12/2 12:21
 */
@Service
public class ReceiveAccountServiceImpl implements ReceiveAccountService {
    @Autowired
    private ReceiveAccountDao receiveAccountDao;

    @Override
    public void addReceiveAccount(ReceiveAccount receiveAccount) {
        receiveAccountDao.addReceiveAccount(receiveAccount);
    }

    @Override
    public void updateReceiveAccount(ObjectId id, ReceiveAccount receiveAccount) {
        Map<String,Object> map = new HashMap<>(6);
        if(!StringUtil.isEmpty(receiveAccount.getUrl())){
            map.put("url",receiveAccount.getUrl());
        }
        if(receiveAccount.getType()!=0){
            map.put("type",receiveAccount.getType());
        }
        if(!StringUtil.isEmpty(receiveAccount.getName())){
            map.put("name",receiveAccount.getName());
        }
        if(!StringUtil.isEmpty(receiveAccount.getPayNo())){
            map.put("payNo",receiveAccount.getPayNo());
        }
        if(!StringUtil.isEmpty(receiveAccount.getBankCard())){
            map.put("bankCard",receiveAccount.getBankCard());
        }
        if(!StringUtil.isEmpty(receiveAccount.getBankName())){
            map.put("bankName",receiveAccount.getBankName());
        }
        receiveAccountDao.updateReceiveAccount(id,map);
    }

    @Override
    public void deleteReceiveAccount(ObjectId id) {
        receiveAccountDao.deleteReceiveAccount(id);
    }

    @Override
    public PageResult<ReceiveAccount> getReceiveAccountList(int pageIndex, int pageSize, int type, String keyword) {
        return receiveAccountDao.getReceiveAccountList(pageIndex,pageSize,type,keyword);
    }

    @Override
    public ReceiveAccount getReceiveAccount(ObjectId id) {
        return receiveAccountDao.getReceiveAccount(id);
    }
}
