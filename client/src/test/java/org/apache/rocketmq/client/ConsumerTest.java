/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.rocketmq.client;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author MUSI
 * @Date 2023/12/1 9:37 PM
 * @Description
 * @Version
 **/
public class ConsumerTest {

    public static void main(String[] args) throws MQClientException {

        Map<String, AtomicInteger> map = new ConcurrentHashMap<>();

        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("consumerGroup");
        pushConsumer.setNamesrvAddr("192.168.1.2:9876");
        pushConsumer.setPullInterval(60 * 1000);
        pushConsumer.subscribe("order_topic_info", "*");
        pushConsumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    String s = new String(msg.getBody());
                    JSONObject jsonObject = JSONObject.parseObject(s, JSONObject.class);
                    String msgId = msg.getMsgId();
                    AtomicInteger atomicInteger = map.putIfAbsent(msgId, new AtomicInteger(0));
                    atomicInteger.addAndGet(1);
                    int count = atomicInteger.get();
                    System.out.println("消息体: " + s + ", 次数: " + count);
                    if (count > 10000) {
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        pushConsumer.start();
    }

    public void popTest() {
    }
}
