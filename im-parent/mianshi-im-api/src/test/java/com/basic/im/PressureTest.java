//package com.basic.im;
//
//import com.basic.im.model.PressureParam;
//import com.basic.im.service.PressureTestManagerImpl;
//import com.basic.im.vo.JSONMessage;
//import com.basic.mianshi.Application;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.test.context.junit4.SpringRunner;
//
//import java.io.IOException;
//import java.util.ArrayList;
//
///**
// * @author zhm
// * @version V1.0
// * @Description: TODO(todo)
// * @date 2020/4/23 18:02
// */
//
//@RunWith(SpringRunner.class)
//@SpringBootTest(classes= Application.class)
//public class PressureTest {
//
//    private static final Logger log = LoggerFactory.getLogger(PressureTest.class);
//
//    @Autowired(required=false)
//    private PressureTestManagerImpl pressureTestManager;
//
//    @Test
//    public void pressureTest() throws IOException {
//        PressureParam param = new PressureParam();
//        param.setRoomJid("e908395097024ac8a2da8f2aa1fc0e4d");
//        ArrayList<String> objects = new ArrayList<>();
//        objects.add("e908395097024ac8a2da8f2aa1fc0e4d");
//        param.setJids(objects);
//        param.setSendMsgNum(10);
//
//
//        JSONMessage jsonMessage = pressureTestManager.mucTest(param, 1000);
//        if(null!=jsonMessage)
//            System.out.println(jsonMessage.toJSONString());
//        jsonMessage = pressureTestManager.mucTest(param, 1000);
//        if(null!=jsonMessage)
//            System.out.println(jsonMessage.toJSONString());
//
//
//        System.in.read();
//    }
//}
