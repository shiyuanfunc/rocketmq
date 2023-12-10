/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.rocketmq.client;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @Author MUSI
 * @Date 2023/12/1 9:05 PM
 * @Description
 * @Version
 **/
public class Test {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("producer");
        producer.setNamesrvAddr("192.168.1.2:9876");
        producer.start();


        for (int i = 1; i < 30; i++) {
            JSONObject params = new JSONObject();
            params.put("msg", "消息内容新排序" + i);
            params.put("count", 1);
            Message message = new Message("order_topic_info", params.toJSONString().getBytes(StandardCharsets.UTF_8));
            SendResult send = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    return mqs.get(0);
                }
            }, "*");
            System.out.println(JSONObject.toJSONString(send));
        }
    }
}
