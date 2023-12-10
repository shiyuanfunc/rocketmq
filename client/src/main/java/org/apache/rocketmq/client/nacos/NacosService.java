/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.rocketmq.client.nacos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.AbstractListener;
import com.alibaba.nacos.api.exception.NacosException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author MUSI
 * @Date 2023/12/10 3:14 PM
 * @Description
 * @Version
 **/
public class NacosService {

    private static final Logger log = LoggerFactory.getLogger(NacosService.class);
    private static ConfigService configService = null;
    private static final Map<String, String> configMap = new ConcurrentHashMap<>();

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    private static String serverAddr = "192.168.1.2:8848";
    private static String namespace = "config_info";
    private static String dataId = "mq_topic";
    private static String group = "DEFAULT_GROUP";

    public void start() {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.RUNNING;
                try {
                    Properties properties = new Properties();
                    properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
                    properties.put(PropertyKeyConst.NAMESPACE, namespace);
                    configService = NacosFactory.createConfigService(properties);
                    String content = configService.getConfigAndSignListener(dataId, group, 3000, new AbstractListener() {
                        @Override
                        public void receiveConfigInfo(String content) {
                            parseJson(content);
                        }
                    });
                    parseJson(content);
                } catch (NacosException nacosException) {
                    log.info("nacos init error, serverAddr: {}, namespace: {}, dataId: {}, group: {}", serverAddr, namespace, dataId, group, nacosException);
                } catch (Exception ex) {
                    log.info("nacos init error ", ex);
                }
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                log.info("nacos init error");
                break;
            default:
                break;
        }
    }

    public void shutdown() {
        if (configService != null) {
            try {
                configService.shutDown();
            } catch (NacosException e) {
                log.info("nacos shutdown error ", e);
            }
        }
    }

    public int getTopicRate(String topic) {
        int topicRate = Integer.MAX_VALUE;
        String rate = configMap.get(topic);
        if (StringUtils.isNotBlank(rate)) {
            try {
                topicRate = Integer.parseInt(rate);
            } catch (Exception ex) {
                // pass
            }
        }
        if (topicRate <= 0) {
            topicRate = Integer.MAX_VALUE;
        }
        return topicRate;
    }

    private static void parseJson(String content) {
        if (StringUtils.isBlank(content)) {
            return;
        }
        synchronized (NacosService.class) {
            try {
                JSONObject jsonObject = JSON.parseObject(content);
                if (MapUtils.isEmpty(jsonObject)) {
                    return;
                }
                configMap.clear();
                jsonObject.forEach((k, v) -> configMap.put(k, v.toString()));
            } catch (Exception ex) {
                log.info("nacos config parse error", ex);
            }
        }
    }
}
