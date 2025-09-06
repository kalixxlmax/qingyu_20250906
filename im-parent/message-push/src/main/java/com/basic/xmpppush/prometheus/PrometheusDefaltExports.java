package com.basic.xmpppush.prometheus;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;

/**
 * @description: PrometheusDefaltExports <br>
 * @date: 2020/9/17 0017  <br>
 * @author: lidaye <br>
 * @version: 1.0 <br>
 */
public class PrometheusDefaltExports {

    private static boolean initialized = false;

    public static synchronized MeterRegistry initialize(String application, int port) {
        MeterRegistry registry = null;
        try {

            if (!initialized) {
                HTTPServer httpServer = new HTTPServer(port, true);
                registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM);
                registry.config().commonTags("application", application);

                jvmRegister(registry);

                initialized = true;
            }

            return registry;


        } catch (IOException e) {
            e.printStackTrace();
            return registry;
        }

    }

    public static MeterRegistry jvmRegister(MeterRegistry registry) {
        if (!initialized) {
            register(registry);
            new ClassLoaderMetrics().bindTo(registry);
            new JvmCompilationMetrics().bindTo(registry);
            new JvmGcMetrics().bindTo(registry);
            new JvmHeapPressureMetrics().bindTo(registry);
            new JvmMemoryMetrics().bindTo(registry);
            new JvmThreadMetrics().bindTo(registry);
        }
        return registry;
    }


    private static void register(MeterRegistry registry){
        CustomIndicator.messageStackedSum(registry);
        CustomIndicator.messageStackedSum(registry);
        CustomIndicator.singleMessageSum(registry);
        CustomIndicator.groupMessageSum(registry);
    }
}
