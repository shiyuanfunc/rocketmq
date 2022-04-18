package org.apache.rocketmq.client.test;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @Author SHI YUAN
 * @DATE 2022/4/18 12:41 AM
 * @Version 1.0
 * @Desc
 */
public class ProducerTest {
    public static void main(String[] args) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("Producer");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long currentTime = System.currentTimeMillis();
        String sendDateStr = simpleDateFormat.format(new Date(currentTime));
        Random random = new Random();
        for (int i = 0; i < 4; i++) {
            int rando = random.nextInt(100000);
            if (rando < 10000){
                rando = 10000;
            }
            Message message = new Message("song_fixed_time", ("this is fixed time message delay time " + i).getBytes());
            long delayTimeAtTime = currentTime + rando;
            message.setDelayTimeAtTime(delayTimeAtTime);
            System.out.println("sendDateStr: " + sendDateStr + ", delayTime: " + (rando / 1000) + "s");
            SendResult sendResult = producer.send(message);
        }
        producer.shutdown();
    }
}
