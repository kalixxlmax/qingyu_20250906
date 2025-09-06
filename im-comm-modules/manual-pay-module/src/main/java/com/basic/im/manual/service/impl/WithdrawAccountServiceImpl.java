package com.basic.im.manual.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.manual.service.WithdrawAccountService;
import com.basic.im.manual.dao.WithdrawAccountDao;
import com.basic.im.manual.entity.WithdrawAccount;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/12/2 17:14
 */
@Service
public class WithdrawAccountServiceImpl implements WithdrawAccountService {

    @Autowired
    private WithdrawAccountDao withdrawAccountDao;

    @Override
    public void addWithdrawAccount(WithdrawAccount entity) {
        entity.setCreateTime(DateUtil.currentTimeSeconds());
        withdrawAccountDao.addWithdrawAccount(entity);
    }

    @Override
    public void deleteWithdrawAccount(ObjectId id) {
        Map<String,Object> map = new HashMap<>(1);
        map.put("status",-1);
        withdrawAccountDao.updateWithdrawAccount(id,map);
    }

    @Override
    public void updateWithdrawAccount(WithdrawAccount withdrawAccount) {
        Map<String,Object> map = new HashMap<>(8);
        if(!StringUtil.isEmpty(withdrawAccount.getAliPayName())){
            map.put("aliPayName",withdrawAccount.getAliPayName());
        }
        if(!StringUtil.isEmpty(withdrawAccount.getAliPayAccount())){
            map.put("aliPayAccount",withdrawAccount.getAliPayAccount());
        }
        if(!StringUtil.isEmpty(withdrawAccount.getCardName())){
            map.put("cardName",withdrawAccount.getCardName());
        }
        if(!StringUtil.isEmpty(withdrawAccount.getBankCardNo())){
            map.put("bankCardNo",withdrawAccount.getBankCardNo());
        }
        if(!StringUtil.isEmpty(withdrawAccount.getBankName())){
            map.put("bankName",withdrawAccount.getBankName());
        }
        if(!StringUtil.isEmpty(withdrawAccount.getBankBranchName())){
            map.put("bankBranchName",withdrawAccount.getBankBranchName());
        }
        if(!StringUtil.isEmpty(withdrawAccount.getDesc())){
            map.put("desc",withdrawAccount.getDesc());
        }
        withdrawAccountDao.updateWithdrawAccount(withdrawAccount.getId(),map);
    }

    @Override
    public PageResult<WithdrawAccount> getWithdrawAccountList(int userId, int pageIndex, int pageSize) {
        return withdrawAccountDao.getWithdrawAccountList(userId,pageIndex,pageSize);
    }

    @Override
    public WithdrawAccount getWithdrawAccount(ObjectId id) {
        return withdrawAccountDao.getWithdrawAccount(id);
    }
}
