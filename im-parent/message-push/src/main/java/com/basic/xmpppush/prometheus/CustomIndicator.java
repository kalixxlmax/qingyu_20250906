package com.basic.xmpppush.prometheus;

import com.basic.xmpppush.server.MessagePushService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.BaseUnits;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * @ClassName CustomIndicator
 * @Author xie yuan yuang
 * @date 2020.09.18 14:19
 * @Description 自定义指标
 */
@Component
public class CustomIndicator {

    public static List<String> str = Collections.synchronizedList(new ArrayList<>());

    //消息堆积数量
    public static void messageStackedSum(MeterRegistry registry) {
        Gauge.builder("message_push_messagestacked_sum", MessagePushService.queue, Queue::size)
                .tags(Collections.emptyList())
                .description("消息堆积数量")
                .baseUnit(BaseUnits.CONNECTIONS)
                .register(registry);
    }

    //发送单聊消息数量
    public static Counter singleMessageSum(MeterRegistry registry) {
        Counter counter = Counter.builder("message_push_singlemessage_sum")
                .tags(Collections.emptyList())
                .description("发送单聊消息数量")
                .baseUnit(BaseUnits.CONNECTIONS)
                .register(registry);
        return counter;
    }

    //发送群聊消息数量
    public static Counter groupMessageSum(MeterRegistry registry) {
        Counter counter = Counter.builder("message_push_groupemessage_sum")
                .tags(Collections.emptyList())
                .description("发送群聊消息数量")
                .baseUnit(BaseUnits.CONNECTIONS)
                .register(registry);
        return counter;
    }

  /*  public static Counter test(MeterRegistry registry) {
        Counter counter = Counter.builder("test_2")
                .tags(Collections.emptyList())
                .description("测试")
                .baseUnit(BaseUnits.CONNECTIONS)
                .register(registry);
        return counter;
    }*/
}
