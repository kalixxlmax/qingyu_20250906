package com.basic.im.admin.service.impl;


import com.basic.im.msg.dao.FxSttingDao;
import com.basic.im.msg.entity.FxSetting;
import com.basic.utils.encrypt.RSA;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.*;
import com.basic.im.admin.entity.*;
import com.basic.im.admin.service.AdminManager;
import com.basic.im.comm.utils.*;
import com.basic.im.utils.ExcelUtil;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.common.service.PaymentManager;
import com.basic.im.dao.SysApiLogDao;
import com.basic.im.entity.*;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.message.MessageService;
import com.basic.im.msg.dao.MusicDao;
import com.basic.im.msg.entity.MusicInfo;
import com.basic.im.open.dao.SkOpenAppDao;
import com.basic.im.open.opensdk.entity.SkOpenApp;
/*import com.basic.im.push.service.FullPushService;
import com.basic.im.push.service.JPushUtils;*/
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.impl.RoomManagerImplForIM;
import com.basic.im.user.dao.InviteCodeDao;
import com.basic.im.user.dao.SdkLoginInfoDao;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.dao.impl.VerifieRequestDaoImpl;
import com.basic.im.user.entity.Certification;
import com.basic.im.user.entity.InviteCode;
import com.basic.im.user.entity.User;
import com.basic.im.user.model.SdkLoginInfo;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.ServiceState;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.util.*;


@Slf4j
@Service
public class AdminManagerImpl implements AdminManager {

    @Autowired
    private ConfigDao configDao;
    @Autowired
    private ClientConfigDao clientConfigDao;
    @Autowired
    private PayConfigDao payConfigDao;
    @Autowired
    private SysApiLogDao sysApiLogDao;
    @Autowired
    private InviteCodeDao inviteCodeDao;
    @Autowired
    private AdminDao adminDao;
    @Autowired
    private ServerListConfigDao serverListConfigDao;
    @Autowired
    private AreaConfigDao areaConfigDao;
    @Autowired
    private UrlConfigDao urlConfigDao;
    @Autowired
    private CenterConfigDao centerConfigDao;
    @Autowired
    private SkOpenAppDao skOpenAppDao;
    @Autowired
    private MusicDao musicDao;
    @Autowired
    private SdkLoginInfoDao sdkLoginInfoDao;
    @Autowired
    private TotalConfigDao totalConfigDao;
    @Autowired
    private UserDao userDao;

    @Autowired
    private UserManagerImpl userManager;

    @Autowired
    private RoomManagerImplForIM roomManager;

    @Autowired
    private FriendsManagerImpl friendsManager;

    @Autowired
    private MessageService messageService;

    @Autowired(required = false)
    private PaymentManager paymentManager;

    @Autowired
    private VerifieRequestDaoImpl verifieRequestDao;

    @Autowired
    private FxSttingDao fxSttingDao;


	/*@Autowired
	private FullPushService fullPushService;

	@Autowired(required = false)
	private JPushUtils jPushUtils;*/

    @Autowired
    private PushConfigDao pushConfigDao;

    public static PushConfig pushConfig;

    @Autowired
    private UploadItemeDao uploadItemeDao;

    @Value("${rocketmq.name-server}")
    private String mqNameServer;


    @Override
    public Config getConfig() {
        Config config = null;
        try {
            config = SKBeanUtils.getImCoreService().getConfig();
            if (null == config) {
                config = configDao.getConfig();
                if (null == config) {
                    config = initConfig();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            config = configDao.getConfig();
        }

        return config;
    }

    @Override
    public ClientConfig getClientConfig() {
        ClientConfig clientconfig = null;
        try {
            clientconfig = KSessionUtil.getClientConfig();
            if (null == clientconfig) {
                clientconfig = clientConfigDao.getClientConfig(10000);
                if (null == clientconfig) {
                    clientconfig = initClientConfig();
                }
                KSessionUtil.setClientConfig(clientconfig);
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            clientconfig = clientConfigDao.getClientConfig(10000);
        }

        return clientconfig;
    }

    @Override
    public SystemApiConfig querySystemApiConfig() {
        return configDao.querySystemApiConfig();
    }

    @Override
    public SystemApiConfig resetSystemApiConfig() {
        SystemApiConfig systemApiConfig = querySystemApiConfig();
        try {
            Map<String, String> keyPair = RSA.generateKeyPair(1024);
            String apiSecret = StringUtil.randomUUID();
            if (null == systemApiConfig) {
                systemApiConfig = new SystemApiConfig(
                        keyPair.get("privateKey"), keyPair.get("publicKey"),
                        apiSecret
                );
            } else {
                systemApiConfig.resetSystemApiConfig(
                        keyPair.get("privateKey"), keyPair.get("publicKey"),
                        apiSecret
                );
            }
        } catch (Exception e) {

        }
        configDao.setSystemApiConfig(systemApiConfig);
        return systemApiConfig;
    }

    @Override
    public PayConfig getPayConfig() {
        PayConfig payConfig = null;
        try {
            payConfig = SKBeanUtils.getImCoreService().getPayConfig();
            if (null == payConfig) {
                payConfig = payConfigDao.getPayConfig();
                if (null == payConfig) {
                    payConfig = initPayConfig();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            payConfig = payConfigDao.getPayConfig();
        }
        return payConfig;
    }

    @Override
    public Config initConfig() {
        Config config = new Config();
        try {
//			config.XMPPDomain="im.server.co";
//			config.setLiveUrl("rtmp://v1.one-tv.com:1935/live/");
            config.setShareUrl("");
            config.setSoftUrl("");
            config.setHelpUrl("");
            config.setVideoLen("20");
            config.setAudioLen("20");
            configDao.addConfig(config);
//				initSystemNo();

            return config;
        } catch (Exception e) {
            e.printStackTrace();
            return config;
        }
    }

//	public void initSystemNo(){
//
//		ThreadUtil.executeInThread(new Callback() {
//
//			@Override
//			public void execute(Object obj) {
//				try {
//					Map<String, String> systemAdminMap = SKBeanUtils.getSystemAdminMap();
//					List<String> mapKeyList = new ArrayList<String>(systemAdminMap.keySet());
//					for(int i = 0; i < mapKeyList.size(); i++){
//						userManager.addUser(Integer.valueOf(mapKeyList.get(i)), systemAdminMap.get(mapKeyList.get(i)));
//						KXMPPServiceImpl.getInstance().registerSystemNo(mapKeyList.get(i), DigestUtils.md5Hex(systemAdminMap.get(mapKeyList.get(i))));
//					}
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//		});
//
//
//
//	}


    @Override
    public void setConfig(Config config) {
        // 开启个人地理位置权限判断全局的地址位置权限是否为开启的反之失败
        if (1 == config.getIsOpenPrivacyPosition()) {
            int isOpenPositionService = getClientConfig().getIsOpenPositionService();
            if (1 == isOpenPositionService) {
                throw new ServiceException("客户端配置中关闭了全局的地理位置权限");
            }
        }
        Config dest = SKBeanUtils.getImCoreService().getConfig();
        BeanUtils.copyProperties(config, dest);

        SKBeanUtils.getImCoreService().setConfig(dest);

    }

    @Override
    public PageResult<SysApiLog> apiLogList(String keyWorld, int page, int limit) {
        PageResult<SysApiLog> result;
        result = sysApiLogDao.getSysApiLog(keyWorld, page, limit);
        return result;
    }

    @Override
    public void deleteApiLog(String apiLogId, int type) {
        if (0 == type) {
            if (StringUtil.isEmpty(apiLogId)) {
                throw new ServiceException("缺少必传参数或,参数错误");
            } else {
                String[] logids = StringUtil.getStringList(apiLogId);
                for (String logid : logids) {
                    sysApiLogDao.deleteSysApiLog(new ObjectId(logid));
                }
            }
        } else if (1 == type) {
            long startTime = DateUtil.currentTimeSeconds();//开始时间（秒）
            long endTime = DateUtil.getOnedayNextDay(startTime, 7, 1); //7天前的时间 ,结束时间（秒）,默认为当前时间
            sysApiLogDao.deleteSysApiLogByTime(endTime);
        }
    }


    /**
     * 生成邀请码
     * num 需要生成的邀请码数量
     *
     * @return
     */
    @Override
    public void createInviteCode(int num, int userId) {
        int totalTimes = 0;

        //当前邀请码标识号
        long curInviteCodeNo = userManager.createInviteCodeNo(num);

        //获取系统当前的邀请码模式 0:关闭   1:开启一对一邀请(一码一用,注册型邀请码)    2:开启一对多邀请(一码多用,推广型邀请码)
        int inviteCodeMode = getConfig().getRegisterInviteCode();
        if (inviteCodeMode == 0) {
            throw new ServiceException("系统当前没有开启邀请码");
        } else if (inviteCodeMode == 1) { //一次
            totalTimes = 1;
        } else if (inviteCodeMode == 2) { //不限次数

            //不限次数类型的邀请码，一个用户只能有一个
            //检查该用户是否存在该类型的邀请码,存在则不再生成
            if (inviteCodeDao.findUserInviteCode(userId) != null) {
                return;
            }
            totalTimes = -1;
            num = 1; //将生成个数设置为1
        } else {
            throw new ServiceException("系统邀请码模式异常");
        }

        String inviteCodeStr = ""; //邀请码
        for (int i = 1; i <= num; i++) {
            inviteCodeStr = RandomUtil.idToSerialCode(DateUtil.currentTimeSeconds() + curInviteCodeNo + i + RandomUtil.getRandomNum(100, 1000)); //生成邀请码
            InviteCode inviteCodeObj = new InviteCode(userId, inviteCodeStr, System.currentTimeMillis(), totalTimes);
            inviteCodeDao.addInviteCode(inviteCodeObj);
        }

    }


    //查询某个邀请码列表
    @Override
    public PageResult<InviteCode> inviteCodeList(int userId, String keyworld, short status, int page, int limit) {
        PageResult<InviteCode> result;
        result = inviteCodeDao.getInviteCodeList(userId, keyworld, status, page, limit);
        return result;
    }

    //删除邀请码
    @Override
    public boolean delInviteCode(int userId, String inviteCodeId) {
        if (StringUtil.isEmpty(inviteCodeId) || userId == 0) {
            throw new ServiceException("请求参数错误或无效");
        }
        ObjectId inviteCode_obId = new ObjectId(inviteCodeId);
        return inviteCodeDao.deleteInviteCode(userId, inviteCode_obId);
    }

    @Override
    public InviteCode findUserPopulInviteCode(int userId) {
        return inviteCodeDao.findUserInviteCode(userId);
    }


    @Override
    public void addAdmin(String account, String password, byte role) {
        Admin admin = new Admin(account, password, role, (byte) 1, System.currentTimeMillis());
        adminDao.addAdmin(admin);
    }

    @Override
    public Admin findAdminByAccount(String account) {
        Admin admin = adminDao.getAdminByAccount(account);
        return admin;
    }

    @Override
    public Admin findAdminById(ObjectId adminId) {
        Admin admin = adminDao.getAdminById(adminId);
        return admin;
    }

    @Override
    public PageResult<Admin> adminList(String keyWorld, ObjectId adminId, int page, int limit) {
        PageResult<Admin> result = adminDao.getAdminList(keyWorld, adminId, page, limit);
        return result;
    }


    @Override
    public void delAdminById(ObjectId adminId) {
        adminDao.deleteAdmin(adminId);
    }


    @Override
    public boolean changePasswd(ObjectId adminId, String newPwd) {
        return adminDao.updateAdminPassword(adminId, newPwd);
    }


    @Override
    public Admin modifyAdmin(Admin admin) {

        Map<String, Object> map = new HashMap<>(4);
        if (!StringUtil.isEmpty(admin.getPassword())) {
            map.put("password", admin.getPassword());
        }

        if (admin.getRole() >= 0) {
            map.put("role", admin.getRole());
        }

        if (admin.getState() >= 0) {
            map.put("state", admin.getState());
        }

        if (0 != admin.getLastLoginTime()) {
            map.put("lastLoginTime", admin.getLastLoginTime());
        }

        return adminDao.updateAdmin(admin.getId(), map);
    }


    @Override
    public ClientConfig initClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        try {
            clientConfig.XMPPDomain = "im.server.com";
            clientConfig.XMPPHost = "im.server.com";
            clientConfig.popularAPP = "{\"lifeCircle\":1,\"videoMeeting\":1,\"liveVideo\":1,\"shortVideo\":1,\"peopleNearby\":1,\"scan\":1}";
            clientConfigDao.addClientConfig(clientConfig);
            //		initSystemNo();

            return clientConfig;
        } catch (Exception e) {
            e.printStackTrace();
            return clientConfig;
        }

    }

    @Override
    public void setClientConfig(ClientConfig config) {
        // 关闭全局的地址位置权限后同时关闭用户隐私设置中的位置权限
        if (1 == config.getIsOpenPositionService()) {
            configDao.updateConfig(0);
        }
        ClientConfig dest = getClientConfig();
        BeanUtils.copyProperties(config, dest);
        SKBeanUtils.getImCoreService().setClientConfig(dest);
    }

    @Override
    public PayConfig initPayConfig() {
        PayConfig payConfig = new PayConfig();
        try {
            payConfig.setMaxTransferAmount(10);
            payConfig.setMaxRedpacktAmount(10);
            payConfig.setMaxRedpacktNumber(10);
            payConfig.setMaxRechargeAmount(10);
            payConfig.setMaxWithdrawAmount(10);
            payConfig.setMaxCodePaymentAmount(10);
            payConfig.setMaxCodeReceiptAmount(10);
            payConfig.setDayMaxRechargeAmount(50);
            payConfig.setDayMaxRedpacktAmount(50);
            payConfig.setDayMaxTransferAmount(50);
            payConfig.setDayMaxWithdrawAmount(50);
            payConfig.setDayMaxCodePayAmount(50);
            payConfig.setDayMaxCodeReceiptAmount(50);
            payConfigDao.addPayConfig(payConfig);
            return payConfig;
        } catch (Exception e) {
            e.printStackTrace();
            return payConfig;
        }
    }

    @Override
    public void setPayConfig(PayConfig dbObj) {
        PayConfig payConfig = getPayConfig();
        BeanUtils.copyProperties(dbObj, payConfig);
        SKBeanUtils.getImCoreService().setPayConfig(payConfig);
    }

    @Override
    public PageResult<ServerListConfig> getServerList(ObjectId id, int pageIndex, int limit) {
        return serverListConfigDao.getServerList(id, pageIndex, limit);
    }

    @Override
    public void addServerList(ServerListConfig server) {
        ServerListConfig serverListConfig = new ServerListConfig();
        if (!StringUtil.isEmpty(server.getName())) {
            serverListConfig.setName(server.getName());
        }
        if (!StringUtil.isEmpty(server.getUrl())) {
            serverListConfig.setUrl(server.getUrl());
        }
        if (!StringUtil.isEmpty(server.getPort())) {
            serverListConfig.setPort(server.getPort());
        }
        if (!StringUtil.isEmpty(server.getArea())) {
            serverListConfig.setArea(server.getArea());
        }
        if (!StringUtil.isEmpty(server.getName())) {
            serverListConfig.setName(server.getName());
        }

        serverListConfig.setCount(server.getCount());
        serverListConfig.setMaxPeople(server.getMaxPeople());
        serverListConfig.setStatus(server.getStatus());
        serverListConfig.setType(server.getType());
        serverListConfigDao.addServerList(serverListConfig);
    }

    @Override
    public void updateServer(ServerListConfig server) {
        Map<String, Object> map = new HashMap<>(6);
        if (!StringUtil.isEmpty(server.getName())) {
            map.put("name", server.getName());
        }
        if (!StringUtil.isEmpty(server.getUrl())) {
            map.put("url", server.getUrl());
        }
        if (!StringUtil.isEmpty(server.getPort())) {
            map.put("port", server.getPort());
        }
        if (0 != server.getCount()) {
            map.put("count", server.getCount());
        }
        if (0 != server.getMaxPeople()) {
            map.put("maxPeople", server.getMaxPeople());
        }
        if (0 != server.getStatus()) {
            map.put("status", server.getStatus());
        }
        serverListConfigDao.updateServer(server.getId(), map);
    }

    @Override
    public PageResult<AreaConfig> areaConfigList(String area, int pageIndex, int limit) {
        return areaConfigDao.getAreaConfigList(area, pageIndex, limit);
    }

    @Override
    public void addAreaConfig(AreaConfig area) {
        if (area.getId() != null) {
            Map<String, Object> map = new HashMap<>(2);
            if (!StringUtil.isEmpty(area.getArea())) {
                map.put("area", area.getArea());
            }
            if (!StringUtil.isEmpty(area.getName())) {
                map.put("name", area.getName());
            }
            areaConfigDao.updateAreaConfig(area.getId(), map);
        } else {
            areaConfigDao.addAreaConfig(area);
        }
    }

    @Override
    public void updateAreaConfig(AreaConfig areaConfig) {
//		Query<AreaConfig> q=getDatastore().createQuery(AreaConfig.class).field("_id").equal(areaConfig.getId());
//		UpdateOperations<AreaConfig> ops=getDatastore().createUpdateOperations(AreaConfig.class);
        Map<String, Object> map = new HashMap<>(2);
        if (!StringUtil.isEmpty(areaConfig.getArea())) {
            map.put("area", areaConfig.getArea());
        }
//			ops.set("area",areaConfig.getArea());
//		if(!StringUtil.isEmpty(areaConfig.getHttpConfig()))
//			ops.set("httpConfig", areaConfig.getHttpConfig());
//		if(!StringUtil.isEmpty(areaConfig.getXmppConfig()))
//			ops.set("xmppConfig", areaConfig.getXmppConfig());
//		if(!StringUtil.isEmpty(areaConfig.getLiveConfig()))
//			ops.set("liveConfig", areaConfig.getLiveConfig());
//		if(!StringUtil.isEmpty(areaConfig.getVideoConfig()))
//			ops.set("videoConfig", areaConfig.getVideoConfig());
        if (!StringUtil.isEmpty(areaConfig.getName())) {
            map.put("name", areaConfig.getName());
        }
//			ops.set("name", areaConfig.getName());
//		if(!StringUtil.isEmpty(areaConfig.getStatus()))
//			ops.set("status", areaConfig.getStatus());

//		getDatastore().findAndModify(q, ops);
        areaConfigDao.updateAreaConfig(areaConfig.getId(), map);
    }

    @Override
    public UrlConfig addUrlConfig(UrlConfig urlConfig) {
        UrlConfig data = new UrlConfig();
        if (urlConfig.getId() != null) {
            Map<String, Object> map = new HashMap<>();
            if (!StringUtil.isEmpty(urlConfig.getArea())) {
                map.put("area", urlConfig.getArea());
            }
            if (!StringUtil.isEmpty(urlConfig.getType())) {
                map.put("type", urlConfig.getType());
            }
            if (!StringUtil.isEmpty(urlConfig.getToArea())) {
                map.put("toArea", urlConfig.getToArea());
            }
            urlConfigDao.updateUrlConfig(urlConfig.getId(), map);
            return urlConfig;
        } else {
            if (!StringUtil.isEmpty(urlConfig.getArea())) {
                data.setArea(urlConfig.getArea());
            }
//			if(!StringUtil.isEmpty(urlConfig.getName()))
//				data.setName(urlConfig.getName());
            if (!StringUtil.isEmpty(urlConfig.getType())) {
                data.setType(urlConfig.getType());
            }
            if (!StringUtil.isEmpty(urlConfig.getToArea())) {
                data.setToArea(urlConfig.getToArea());
            }
//			if(urlConfig.getIds().size()!=0)
//				data.setIds(urlConfig.getIds());

            return urlConfigDao.addUrlConfig(data);
        }

    }

    @Override
    public PageResult<UrlConfig> findUrlConfig(ObjectId id, String type) {
        return urlConfigDao.getUrlConfigList(id, type);
    }

    @Override
    public CenterConfig addCenterConfig(CenterConfig centerConfig) {
        CenterConfig data = new CenterConfig();
        if (centerConfig.getId() != null) {
            Map<String, Object> map = new HashMap<>();
            if (!StringUtil.isEmpty(centerConfig.getClientA())) {
                map.put("clientA", centerConfig.getClientA());
            }
            if (!StringUtil.isEmpty(centerConfig.getClientB())) {
                map.put("clientB", centerConfig.getClientB());
            }
            if (!StringUtil.isEmpty(centerConfig.getArea())) {
                map.put("area", centerConfig.getArea());
            }
            if (!StringUtil.isEmpty(centerConfig.getName())) {
                map.put("name", centerConfig.getName());
            }
            if (centerConfig.getStatus() != 0) {
                map.put("status", centerConfig.getStatus());
            }
            if (!StringUtil.isEmpty(centerConfig.getType())) {
                map.put("type", centerConfig.getType());
            }

            centerConfigDao.updateCenterConfig(centerConfig.getId(), map);
            return centerConfig;
        } else {
            if (!StringUtil.isEmpty(centerConfig.getClientA())) {
                data.setClientA(centerConfig.getClientA());
            }
            if (!StringUtil.isEmpty(centerConfig.getClientB())) {
                data.setClientB(centerConfig.getClientB());
            }
            if (!StringUtil.isEmpty(centerConfig.getArea())) {
                data.setArea(centerConfig.getArea());
            }
            data.setName(centerConfig.getName());
            data.setStatus(centerConfig.getStatus());
            data.setType(centerConfig.getType());

            return centerConfigDao.addCenterConfig(data);
        }
    }

    @Override
    public PageResult<CenterConfig> findCenterConfig(String type, ObjectId id) {

        return centerConfigDao.getCenterConfig(type, id);
    }

    @Override
    public void deleteServer(ObjectId id) {
        serverListConfigDao.deleteServer(id);
    }

    @Override
    public void addTotalConfig(TotalConfig totalConfig) {
        TotalConfig data = new TotalConfig();
        if (totalConfig.getId() != null) {
            Map<String, Object> map = new HashMap<>();
            if (!StringUtil.isEmpty(totalConfig.getArea())) {
                map.put("", totalConfig.getArea());
            }
//				ops.set("", totalConfig.getArea());
            if (!StringUtil.isEmpty(totalConfig.getHttpConfig())) {
                map.put("", totalConfig.getHttpConfig());
            }
//				ops.set("", totalConfig.getHttpConfig());
            if (!StringUtil.isEmpty(totalConfig.getXmppConfig())) {
                map.put("", totalConfig.getXmppConfig());
            }
//				ops.set("", totalConfig.getXmppConfig());
            if (!StringUtil.isEmpty(totalConfig.getLiveConfig())) {
                map.put("", totalConfig.getLiveConfig());
            }
//				ops.set("", totalConfig.getLiveConfig());
            if (!StringUtil.isEmpty(totalConfig.getVideoConfig())) {
                map.put("", totalConfig.getVideoConfig());
            }
//				ops.set("", totalConfig.getVideoConfig());
            if (!StringUtil.isEmpty(totalConfig.getName())) {
                map.put("", totalConfig.getName());
            }
//				ops.set("", totalConfig.getName());
            if (totalConfig.getStatus() != 0) {
                map.put("", totalConfig.getStatus());
            }
//				ops.set("", totalConfig.getStatus());

//			getDatastore().findAndModify(q, ops);
            totalConfigDao.updateTotalConfig(totalConfig.getId(), map);

        } else {
            if (!StringUtil.isEmpty(totalConfig.getArea())) {
                data.setArea(totalConfig.getArea());
            }
            ;
            if (!StringUtil.isEmpty(totalConfig.getHttpConfig())) {
                data.setHttpConfig(totalConfig.getHttpConfig());
            }
            if (!StringUtil.isEmpty(totalConfig.getXmppConfig())) {
                data.setXmppConfig(totalConfig.getXmppConfig());
            }
            if (!StringUtil.isEmpty(totalConfig.getLiveConfig())) {
                data.setLiveConfig(totalConfig.getLiveConfig());
            }
            if (!StringUtil.isEmpty(totalConfig.getVideoConfig())) {
                data.setVideoConfig(totalConfig.getVideoConfig());
            }
            if (!StringUtil.isEmpty(totalConfig.getName())) {
                data.setName(totalConfig.getName());
            }
            if (totalConfig.getStatus() != 0) {
                data.setStatus(totalConfig.getStatus());
            }

//			getDatastore().save(data);
            totalConfigDao.addTotalConfig(data);
        }

    }

    @Override
    public void deleteUrlConfig(ObjectId id) {
//		Query<UrlConfig> query=getDatastore().createQuery(UrlConfig.class).field("_id").equal(id);
//		getDatastore().delete(query);
        urlConfigDao.deleteUrlConfig(id);
    }

    @Override
    public PageResult<ServerListConfig> findServerByArea(String area) {
//		Query<ServerListConfig> query=getDatastore().createQuery(ServerListConfig.class);
//		query.field("area").equal(area);
//		PageResult<ServerListConfig> result=new PageResult<>();
//		result.setCount(query.count());
//		result.setData(query.asList());
//		return result;
        return serverListConfigDao.getServerListByArea(area);
    }

    @Override
    public void deleteAreaConfig(ObjectId id) {
//		Query<AreaConfig> query=getDatastore().createQuery(AreaConfig.class).field("_id").equal(id);
//		getDatastore().delete(query);
        areaConfigDao.deleteAreaConfig(id);
    }

    @Override
    public UrlConfig findUrlConfig(String area) {
//		Query<UrlConfig> query=getDatastore().createQuery(UrlConfig.class).field("area").equal(area);
//		return query.get();
        return urlConfigDao.getUrlConfig(area);
    }


    public String getArea(String area) {
        return area.split(",")[0];
    }

    /**
     * 集群分配服务器（轮询）
     *
     * @param
     * @return server
     */

    public synchronized ConfigVO serverDistribution(String area, ConfigVO configVO) {
        area = getArea(area);
        UrlConfig urlconfig = findUrlConfig(area);
        PageResult<ServerListConfig> result;

        if (urlconfig != null) {
            result = findServerByArea(urlconfig.getArea());
        } else {
            result = findServerByArea("*");
        }

        List<String> xmppList = new ArrayList<>();
        List<String> httpList = new ArrayList<>();
        List<String> videoList = new ArrayList<>();
        List<String> liveList = new ArrayList<>();

        for (ServerListConfig serverListConfig : result.getData()) {
            if (serverListConfig.getType() == KConstants.CLUSTERKEY.XMPP) {
                xmppList.add(serverListConfig.getUrl());
            } else if (serverListConfig.getType() == KConstants.CLUSTERKEY.HTTP) {
                httpList.add(serverListConfig.getUrl());
            } else if (serverListConfig.getType() == KConstants.CLUSTERKEY.VIDEO) {
                videoList.add(serverListConfig.getUrl());
            } else if (serverListConfig.getType() == KConstants.CLUSTERKEY.LIVE) {
                liveList.add(serverListConfig.getUrl());
            }
        }
        int random = 0;
        // 轮询xmpp服务器
        if (0 < xmppList.size()) {
            if (1 == xmppList.size()) {
                configVO.setXMPPHost(xmppList.get(0));
            } else {
                random = NumberUtil.getNum(0, xmppList.size() - 1);
                configVO.setXMPPHost(xmppList.get(random));
            }
        }
        if (0 < httpList.size()) {
            // 轮询http服务器
            if (1 == httpList.size()) {
                configVO.setApiUrl(httpList.get(0));
            } else {
                random = NumberUtil.getNum(0, httpList.size() - 1);
                configVO.setApiUrl(httpList.get(random));
            }
        }


        if (0 < videoList.size()) {
            // 轮询视频服务器
            if (1 == videoList.size()) {
                configVO.setJitsiServer(videoList.get(0));
            } else {
                random = NumberUtil.getNum(0, videoList.size() - 1);
                configVO.setJitsiServer(videoList.get(random));
            }
        }
        if (0 < liveList.size()) {
            // 轮询直播服务器
            if (1 == liveList.size()) {
                configVO.setLiveUrl(liveList.get(0));
            } else {
                random = NumberUtil.getNum(0, liveList.size() - 1);
                configVO.setLiveUrl(liveList.get(random));
            }
        }

        return configVO;
    }

    @Override
    public void deleteCenter(ObjectId id) {
//		Query<CenterConfig> query=getDatastore().createQuery(CenterConfig.class).field("_id").equal(id);
//		getDatastore().delete(query);
        centerConfigDao.deleteCenterConfig(id);
    }

    @Override
    public CenterConfig findCenterCofigByArea(String clientA, String clientB) {
        if (null == clientB) {
            clientB = "CN";
        }
//		Query<CenterConfig> query=getDatastore().createQuery(CenterConfig.class).filter("clientA", getArea(clientA)).filter("clientB", getArea(clientB));
        CenterConfig centerConfig = centerConfigDao.getCenterConfig(getArea(clientA), getArea(clientB));
        if (centerConfig == null) {
//			query=getDatastore().createQuery(CenterConfig.class).filter("clientA", getArea(clientB)).filter("clientB", getArea(clientA));
            centerConfig = centerConfigDao.getCenterConfig(getArea(clientB), getArea(clientA));
        }

        return centerConfig;
    }

    @Override
    public PageResult<SkOpenApp> openAppList(int status, int type, int pageIndex, int limit, String keyword) {
//		Query<SkOpenApp> query=getDatastore().createQuery(SkOpenApp.class).field("appType").equal(type);
//		if(status==0){
//			query.field("status").equal(status);
//		}
//		if(!StringUtil.isEmpty(keyword)){
//			query.or(query.criteria("appName").contains(keyword));
//		}
//		PageResult<SkOpenApp> result=new PageResult<SkOpenApp>();
//		result.setCount(query.count());
//		result.setData(query.asList(pageFindOption(pageIndex, limit, 1)));
//
//		return result;
        return skOpenAppDao.getSkOpenAppList(status, type, pageIndex, limit, keyword);
    }

    /**
     * 查询音乐
     *
     * @param pageIndex
     * @param pageSize
     * @param keyword
     * @return
     */
    public PageResult<MusicInfo> queryMusicInfo(int pageIndex, int pageSize, String keyword) {
//		PageResult<MusicInfo> result=new PageResult<>();
//		Query<MusicInfo> query=getDatastore().createQuery(MusicInfo.class);
//		if(!StringUtil.isEmpty(keyword))
//			query.or(query.criteria("name").contains(keyword),
//					query.criteria("nikeName").contains(keyword));
//		query.order("-useCount");
//		result.setCount(query.count());
//		result.setData(query.asList(pageFindOption(pageIndex, pageSize,1)));
//		return result;
        return musicDao.getMusicInfo(pageIndex, pageSize, keyword);
    }

    /**
     * 查询转账记录
     *
     * @param pageIndex
     * @param pageSize
     * @param keyword
     * @return
     */
    public PageResult<Transfer> queryTransfer(int pageIndex, int pageSize, String keyword, String startDate, String endDate) {
        if (paymentManager == null) {
            throw new ServiceException(KConstants.ResultCode.CLOSEPAY);
        }
        return (PageResult<Transfer>) paymentManager.getTransferList(pageIndex, pageSize, keyword, startDate, endDate);
    }

    /**
     * 获取第三方绑定列表
     *
     * @param pageIndex
     * @param pageSize
     * @param keyword
     * @return
     */
    public PageResult<SdkLoginInfo> getSdkLoginInfoList(int pageIndex, int pageSize, String keyword) {
//		PageResult<SdkLoginInfo> result = new PageResult<>();
//		Query<SdkLoginInfo> query = getDatastore().createQuery(SdkLoginInfo.class);
//		if(!StringUtil.isEmpty(keyword))
//			query.field("userId").equal(Integer.valueOf(keyword));
//		query.order("-createTime");
//		result.setCount(query.count());
//		result.setData(query.asList(pageFindOption(pageIndex, pageSize, 1)));
//		return result;
        return sdkLoginInfoDao.getSdkLoginInfoList(pageIndex, pageSize, keyword);
    }

    /**
     * 删除第三方绑定
     *
     * @param id
     */
    public void deleteSdkLoginInfo(ObjectId id) {
//		Query<SdkLoginInfo> query = getDatastore().createQuery(SdkLoginInfo.class).field("_id").equal(id);
//		getDatastore().delete(query);
        sdkLoginInfoDao.deleteSdkLoginInfo(id);
    }

    /**
     * 删除该用户所有绑定的第三方信息
     *
     * @param userId
     */
    public void deleteSdkLoginInfoByUserId(int userId) {
        sdkLoginInfoDao.deleteSdkLoginInfoByUserId(userId);
    }

    private DefaultMQProducer pushProducer;

    public DefaultMQProducer getPushProducer() {
        if (null != pushProducer) {
            return pushProducer;
        }

        try {
            pushProducer = new DefaultMQProducer("pushProducer");
            pushProducer.setNamesrvAddr(mqNameServer);
            pushProducer.setVipChannelEnabled(false);
            pushProducer.setCreateTopicKey("fullPushMessage");
            pushProducer.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return pushProducer;
    }

    public void restartProducer() {
        System.out.println("pushProducer restartProducer ===》 " + mqNameServer);
        try {
            if (null != pushProducer && null != pushProducer.getDefaultMQProducerImpl()) {
                if (ServiceState.CREATE_JUST == pushProducer.getDefaultMQProducerImpl().getServiceState()) {
                    try {
                        pushProducer.start();
                    } catch (Exception e) {
                        pushProducer = null;
                        getPushProducer();
                    }
                }
            } else {
                pushProducer = null;
                getPushProducer();
            }
        } catch (Exception e) {
            System.err.println("restartProducer Exception " + e.getMessage());

        }

    }

    /**
     * @param request
     * @param response
     * @return
     * @Description:筛选导出手机号
     **/
    public Workbook exprotExcelPhone(String startDate, String endDate, String onlinestate, String keyWord, HttpServletRequest request, HttpServletResponse response) {
        String name = "手机号明细";
        String fileName = DateUtil.getMDString() + ".xlsx";
        long startTime = 0; //开始时间（秒）
        long endTime = 0; //结束时间（秒）,默认为当前时间
        long formateEndtime = 0;
        if (!StringUtil.isEmpty(startDate) && !StringUtil.isEmpty(endDate)) {
            fileName = new StringBuffer(startDate).substring(5) + "~" + new StringBuffer(endDate).substring(5) + ".xlsx";

            startTime = StringUtil.isEmpty(startDate) ? 0 : DateUtil.toDate(startDate).getTime() / 1000;
            endTime = StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime() / 1000;
            formateEndtime = DateUtil.getOnedayNextDay(endTime, 1, 0);
//			query.field("createTime").greaterThan(startTime).field("createTime").lessThanOrEq(formateEndtime);
        }
        List<User> users = userDao.exprotExcelPhone(startTime, formateEndtime, onlinestate, keyWord);
        List<String> titles = Lists.newArrayList();
        titles.add("姓名");
       // titles.add("公司名称");
        titles.add("电话号码");
       // titles.add("网址");
        titles.add("注册日期");
        List<Map<String, Object>> values = Lists.newArrayList();
        for (User user : users) {
            // 过滤系统账号
            if (StringUtil.isEmpty(user.getTelephone())) {
                continue;
            }
            Map<String, Object> map = Maps.newConcurrentMap();
            map.put("姓名", user.getNickname());
          //  map.put("公司名称", "");
            map.put("电话号码", user.getTelephone());
          //  map.put("网址", "");
            map.put("注册日期", DateUtil.timestamp2Date(String.valueOf(user.getCreateTime()), "yyyy-MM-dd"));
//			map.put("createTime", DateUtil.timestamp2Date(String.valueOf(user.getCreateTime()), "yyyy-MM-dd"));//user.getCreateTime()
            values.add(map);
        }
        Workbook workBook = ExcelUtil.generateWorkbook(name, "xlsx", titles, values);
        response.reset();
        try {
            response.setHeader("Content-Disposition",
                    "attachment;filename=" + new String(fileName.getBytes(), "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return workBook;
    }

    /**
     * @param userId
     * @param request
     * @param response
     * @return
     * @Description: 导出好友列表
     **/
    public Workbook exprotExcelFriends(Integer userId, HttpServletRequest request, HttpServletResponse response) {

        String name = userManager.getNickName(userId) + "的好友明细";

        String fileName = "friends.xlsx";

        List<Friends> friends;

        List<Friends> friendsList = friendsManager.queryFriends(userId);

        List<String> titles = Lists.newArrayList();
        titles.add("toUserId");
        titles.add("toNickname");
        titles.add("remarkName");
        titles.add("telephone");
        titles.add("status");
        titles.add("blacklist");
        titles.add("isBeenBlack");
        titles.add("offlineNoPushMsg");
        titles.add("createTime");

        List<Map<String, Object>> values = Lists.newArrayList();
        for (Friends friend : friendsList) {
            // 过滤系统10000号不返回
            if (10000 == friend.getToUserId()) {
                continue;
            }
            Map<String, Object> map = Maps.newConcurrentMap();
            map.put("toUserId", friend.getToUserId());
            map.put("toNickname", friend.getToNickname());
            map.put("telephone", userManager.getUser(friend.getToUserId()).getPhone());
            map.put("status", friend.getStatus() == -1 ? "黑名单" : friend.getStatus() == 2 ? "好友" : "关注");
            map.put("blacklist", friend.getBlacklist() == 1 ? "是" : "否");
            map.put("isBeenBlack", friend.getIsBeenBlack() == 1 ? "是" : "否");
            map.put("offlineNoPushMsg", friend.getBlacklist() == 1 ? "是" : "否");
            map.put("createTime", DateUtil.strToDateTime(friend.getCreateTime()));
            values.add(map);
        }

        Workbook workBook = ExcelUtil.generateWorkbook(name, "xlsx", titles, values);
        response.reset();
        try {
            response.setHeader("Content-Disposition",
                    "attachment;filename=" + new String(fileName.getBytes(), "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return workBook;
    }


    /**
     * @param roomId
     * @param request
     * @param response
     * @return
     * @Description: 导出群成员
     **/
    public Workbook exprotExcelGroupMembers(String roomId, HttpServletRequest request, HttpServletResponse response) {

        String name = roomManager.getRoomName(new ObjectId(roomId)) + " 的群成员明细";

        String fileName = "groupMembers.xlsx";

        List<Room.Member> members = roomManager.getMemberList(new ObjectId(roomId), null);
        List<String> titles = Lists.newArrayList();
        titles.add("userId");
        titles.add("userName");
        titles.add("remarkName");
        titles.add("telephone");
        titles.add("role");
        titles.add("offlineNoPushMsg");
        titles.add("createTime");
        titles.add("modifyTime");

        List<Map<String, Object>> values = Lists.newArrayList();
        members.forEach(member -> {
            Map<String, Object> map = Maps.newHashMap();
            map.put("userId", member.getUserId());
            map.put("userName", member.getNickname());
            map.put("remarkName", member.getRemarkName());
            map.put("telephone", userManager.getUser(member.getUserId()).getPhone());
            map.put("role", member.getRole() == 1 ? "群主" : member.getRole() == 2 ? "管理员" : member.getRole() == 3 ? "普通成员" : member.getRole() == 4 ? "隐身人" : "监护人");// 1=创建者、2=管理员、3=普通成员、4=隐身人、5=监控人
            map.put("offlineNoPushMsg", member.getOfflineNoPushMsg() == 0 ? "否" : "是");
            map.put("createTime", DateUtil.strToDateTime(member.getCreateTime()));
            map.put("modifyTime", DateUtil.strToDateTime(member.getModifyTime()));
            values.add(map);
        });

        Workbook workBook = ExcelUtil.generateWorkbook(name, "xlsx", titles, values);
        response.reset();
        try {
            response.setHeader("Content-Disposition",
                    "attachment;filename=" + new String(fileName.getBytes(), "utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return workBook;
    }

    /**
     * 发送消息 到群组中
     *
     * @param toUserId
     * @param msgType
     * @param content
     */
    public void sendMsgToUser(int toUserId, int msgType, String content) {
        User user = userManager.getUser(toUserId);
        MessageBean messageBean = new MessageBean();
        messageBean.setFromUserId("10000");
      //  messageBean.setFromUserName("Customer service public account");
        messageBean.setFromUserName("客服公众号");
        messageBean.setToUserId(String.valueOf(toUserId));
        messageBean.setToUserName(user.getNickname());
        messageBean.setType(msgType);
        messageBean.setImSys((short) 1);
        messageBean.setContent(content);
        messageBean.setMessageId(StringUtil.randomUUID());
        messageService.send(messageBean);
    }

    public Certification queryCertification(int type, Integer userId) {
        return verifieRequestDao.queryCertifiedInfo(type, userId);
    }

    /**
     * 全局短信配置
     *
     * @param smsConfig
     */
    @Override
    public void setSmsConfig(SmsConfig smsConfig) {
        SKBeanUtils.getImCoreService().setSmsConfig(smsConfig);
    }

    /**
     * 全局短信配置
     *
     * @param smsConfig_
     */
    @Override
    public void setAppliactionSmsConfig(SmsConfig smsConfig_) {
        SmsConfig smsConfig = SKBeanUtils.getImCoreService().getSmsConfig();
        smsConfig.setOpenSMS(smsConfig_.getOpenSMS());
        SKBeanUtils.getImCoreService().setSmsConfig(smsConfig);
    }

    /**
     * 阿里云短信配置
     *
     * @param smsConfig_
     */
    @Override
    public void setAliyunSmsConfig(SmsConfig smsConfig_) {
        SmsConfig smsConfig = SKBeanUtils.getImCoreService().getSmsConfig();
        smsConfig.setProduct(smsConfig_.getProduct());
        smsConfig.setDomain(smsConfig_.getDomain());
        smsConfig.setAccesskeyid(smsConfig_.getAccesskeyid());
        smsConfig.setAccesskeysecret(smsConfig_.getAccesskeysecret());
        smsConfig.setSignname(smsConfig_.getSignname());
        smsConfig.setChinase_templetecode(smsConfig_.getChinase_templetecode());
        smsConfig.setInternational_templetecode(smsConfig_.getInternational_templetecode());
        smsConfig.setCloudWalletVerification(smsConfig_.getCloudWalletVerification());
        smsConfig.setCloudWalletNotification(smsConfig_.getCloudWalletNotification());
        smsConfig.setBizType(smsConfig_.getBizType());
        SKBeanUtils.getImCoreService().setSmsConfig(smsConfig);
    }

    /**
     * 天天国际短信配置
     *
     * @param smsConfig_
     */
    @Override
    public void setTistiloSmsConfig(SmsConfig smsConfig_) {
        SmsConfig smsConfig = SKBeanUtils.getImCoreService().getSmsConfig();
        smsConfig.setHost(smsConfig_.getHost());
        smsConfig.setPort(smsConfig_.getPort());
        smsConfig.setApi(smsConfig_.getApi());
        smsConfig.setUsername(smsConfig_.getUsername());
        smsConfig.setPassword(smsConfig_.getPassword());
        smsConfig.setTemplateChineseSMS(smsConfig_.getTemplateChineseSMS());
        smsConfig.setTemplateEnglishSMS(smsConfig_.getTemplateEnglishSMS());
        smsConfig.setSmsSendCount(smsConfig_.getSmsSendCount());
        smsConfig.setSmsSendTime(smsConfig_.getSmsSendTime());
        smsConfig.setSmsSendBlackCount(smsConfig_.getSmsSendBlackCount());
        smsConfig.setSmsSendBlackTime(smsConfig_.getSmsSendBlackTime());
        smsConfig.setAllSmsSendCount(smsConfig_.getAllSmsSendCount());
        smsConfig.setAllSmsSendTime(smsConfig_.getAllSmsSendTime());
        SKBeanUtils.getImCoreService().setSmsConfig(smsConfig);
    }

    /**
     * @Description 随机获取一个正整数
     * @Date 15:02 2020/8/11
     **/
    @Override
    public int getRandomIntId() {
        int id = 0;
        boolean flag = true;
        while (flag) {
            int ran = new Random().nextInt();
            if (ran <= 0) {
                continue;
            }
            PushConfig pushConfig = pushConfigDao.getPushConfigModelDetail(ran);
            if (pushConfig == null) {
                id = ran;
                flag = false;
            }
        }
        ;
        return id;
    }

    /**
     * 获取资源服务器 文件内容
     **/
    @Override
    public PageResult<UploadItem> listResource(int pageIndex, int pageSize, String keyword, String fileType, String startTime, String endTime) {
        PageResult<UploadItem> resourcelist;
        try {
            resourcelist = uploadItemeDao.resourcelist(pageIndex, pageSize, keyword, fileType, startTime, endTime);
        } catch (Exception e) {
            resourcelist = new PageResult<>();
            log.info(e.getMessage(), e);
        }

        return resourcelist;
    }

    @Override
    public void deleteResource(String id) {
        uploadItemeDao.deleteResource(new ObjectId(id));
    }

    /**
     * 删除用户在商城中的数据
     **/
    @Override
    public void deleteMallUser(long userId) {
        adminDao.deleteShop(userId);
    }

    /**
     * 设置服务端参数——运行参数
     */
    @Override
    public void setRunningConfig(Config config, String requestApiList) {
        Config thisConfig = SKBeanUtils.getImCoreService().getConfig();
        thisConfig.setXMPPTimeout(config.getXMPPTimeout());
        thisConfig.setIsOpenOnlineStatus(config.getIsOpenOnlineStatus());
        thisConfig.setIsOpenSMSCode(config.getIsOpenSMSCode());
        thisConfig.setIsOpenCluster(config.getIsOpenCluster());
        thisConfig.setIsAuthApi(config.getIsAuthApi());
        thisConfig.setTelephoneSearchUser(config.getTelephoneSearchUser());
        thisConfig.setNicknameSearchUser(config.getNicknameSearchUser());
        thisConfig.setRegeditPhoneOrName(config.getRegeditPhoneOrName());
        thisConfig.setUserNicknameNotword(config.getUserNicknameNotword());
        thisConfig.setRegisterInviteCode(config.getRegisterInviteCode());
        thisConfig.setIsKeyWord(config.getIsKeyWord());
        thisConfig.setIsSaveMsg(config.getIsSaveMsg());
        thisConfig.setIsSaveMucMsg(config.getIsSaveMucMsg());
        thisConfig.setMsgDelayedDeleteTime(config.getMsgDelayedDeleteTime());
        thisConfig.setIsSaveSystemMsg(config.getIsSaveSystemMsg());
        thisConfig.setIsOpenGoogleFCM(config.getIsOpenGoogleFCM());
        thisConfig.setSMSType(config.getSMSType());
        thisConfig.setImgVerificationCode(config.getImgVerificationCode());
        thisConfig.setIsAutoAddressBook(config.getIsAutoAddressBook());
        thisConfig.setIsSaveRequestLogs(config.getIsSaveRequestLogs());
        thisConfig.setGiftRatio(config.getGiftRatio());
        thisConfig.setPromotionUrl(config.getPromotionUrl());
        thisConfig.setDefaultTelephones(config.getDefaultTelephones());
        thisConfig.setDefaultGroups(config.getDefaultGroups());
        thisConfig.setPrivacyPolicyPrefix(config.getPrivacyPolicyPrefix());
        thisConfig.setCreateCompamyIsNeedCheck(config.getCreateCompamyIsNeedCheck());
        thisConfig.setUserJoinCompanyIsNeedManagerConfirm(config.getUserJoinCompanyIsNeedManagerConfirm());
        thisConfig.setInviteJoinCompanyIsNeedUserConfirm(config.getInviteJoinCompanyIsNeedUserConfirm());
        thisConfig.setMaxCrowdNumber(config.getMaxCrowdNumber());
        thisConfig.setBanComment(config.getBanComment());
        thisConfig.setResourceDatabaseUrl(config.getResourceDatabaseUrl());
        thisConfig.setPublishMsgWhetherCheck(config.getPublishMsgWhetherCheck());
        thisConfig.setPublishMsgLableWhetherCheck(config.getPublishMsgLableWhetherCheck());
        thisConfig.setLocationModiyTime(config.getLocationModiyTime());
        thisConfig.setResourceDatabaseUrl(config.getResourceDatabaseUrl());
        thisConfig.setEmployeeTemplateUrl(config.getEmployeeTemplateUrl());
        thisConfig.setDepartmentTemplateUrl(config.getDepartmentTemplateUrl());
        thisConfig.setOpenAdminLoginCode(config.getOpenAdminLoginCode());
        SystemApiConfig systemApiConfig = thisConfig.getSystemApiConfig();
        systemApiConfig.setRequestApiList(StringUtil.getListBySplit(requestApiList, ","));
        thisConfig.setSystemApiConfig(systemApiConfig);
        thisConfig.setGoogleVerification(config.getGoogleVerification());
        thisConfig.setVideoVoice(config.getVideoVoice());
        thisConfig.setRegisterInviteCodeSetting(config.getRegisterInviteCodeSetting());
        thisConfig.setMuteTime(config.getMuteTime());
        thisConfig.setMuteTimeUnit(config.getMuteTimeUnit());
        setConfig(thisConfig);
    }

    /**
     * 设置服务端参数——用户默认设置
     */
    @Override
    public void setUserDefaultConfig(Config config) {
        Config thisConfig = SKBeanUtils.getImCoreService().getConfig();
        thisConfig.setRoamingTime(config.getRoamingTime());
        thisConfig.setOutTimeDestroy(config.getOutTimeDestroy());
        thisConfig.setIsFriendsVerify(config.getIsFriendsVerify());
        thisConfig.setIsMultiLogin(config.getIsMultiLogin());
        thisConfig.setIsVibration(config.getIsVibration());
        thisConfig.setIsTyping(config.getIsTyping());
        thisConfig.setIsUseGoogleMap(config.getIsUseGoogleMap());
        thisConfig.setPhoneSearch(config.getPhoneSearch());
        thisConfig.setNameSearch(config.getNameSearch());
        thisConfig.setIsKeepalive(config.getIsKeepalive());
        thisConfig.setIsOpenPrivacyPosition(config.getIsOpenPrivacyPosition());
        thisConfig.setIsShowMsgState(config.getIsShowMsgState());
        thisConfig.setShowLastLoginTime(config.getShowLastLoginTime());
        thisConfig.setShowTelephone(config.getShowTelephone());
        thisConfig.setGoogleVerification(config.getGoogleVerification());
        thisConfig.setVideoVoice(config.getVideoVoice());
        setConfig(thisConfig);
    }

    /**
     * 设置服务端参数——建立群组默认参数
     */
    @Override
    public void setRoomDefaultConfig(Config config) {
        Config thisConfig = SKBeanUtils.getImCoreService().getConfig();
        thisConfig.setMaxUserSize(config.getMaxUserSize());
        thisConfig.setAdminMaxNumber(config.getAdminMaxNumber());
        thisConfig.setIsLook(config.getIsLook());
        thisConfig.setShowRead(config.getShowRead());
        thisConfig.setIsNeedVerify(config.getIsNeedVerify());
        thisConfig.setIsAttritionNotice(config.getIsAttritionNotice());
        thisConfig.setShowMember(config.getShowMember());
        thisConfig.setAllowSendCard(config.getAllowSendCard());
        thisConfig.setAllowInviteFriend(config.getAllowInviteFriend());
        thisConfig.setAllowUploadFile(config.getAllowUploadFile());
        thisConfig.setAllowConference(config.getAllowConference());
        thisConfig.setAllowSpeakCourse(config.getAllowSpeakCourse());
        thisConfig.setAllowSpeakCourse(config.getAllowSpeakCourse());
        setConfig(thisConfig);
    }

    @Override
    public PageResult<FxSetting> fxList(int pageIndex, Integer pageSize) {
        return fxSttingDao.getMusicInfo(pageIndex,pageSize);
    }


    /**
     * 酷信排除以下条件之外的用户注销
     * 1， 公司同事
     * 2， 18938880745 这个账号创建及管理的群中的群成员
     * 3， 最近一个月注册的用户
     */
    public void destoryAppointUser(int targetUserId) {

		/*//酷信同事 userId 数据
		int[] fellowUserIds = {10042317, 10008291, 10041361, 10004476, 10003505,10027047, 10033990 ,
				10017332, 10039886, 10022588, 10040740, 10039592, 10027282, 10002092, 10040708, 10007042,
				10002984, 10004541, 10008295, 10010521, 10026994, 10022918, 10037273, 10040704, 10003910, 10031316};

		for (int i = 0; i < fellowUserIds.length; i++) {
			int userId = fellowUserIds[i];
			System.out.println(" === 更新数据 "+ i + "=====>>"+userId );
			userDao.updateNotDelete(userId,1);
		}

		//查找出 18938880745 这个用户创建的和管理的群组
		roomManager.getUsersCreateRoomAndManagerRoom(targetUserId);

		List<Integer> allMemberIds = roomManager.getUsersCreateRoomAndManagerRoom(targetUserId);
		for (int i = 0; i < allMemberIds.size(); i++) {
			int userId = allMemberIds.get(i);
			System.out.println(" === 更新数据 "+ i + "=====>>"+userId );
			userDao.updateNotDelete(userId,1);
		}*/


        userManager.deleteUser(1005, userDao.findUserNoDelete());

    }
}
