package org.apache.rocketmq.store.schedule;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author SHI YUAN
 * @DATE 2022/4/17 5:03 PM
 * @Version 1.0
 * @Desc 定时消息
 */

public class FixedTimeMessageService extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 消息处理器
     */
    private final DefaultMessageStore defaultMessageStore;

    private final ScheduledExecutorService scheduledExecutorService;

    private final ScheduledExecutorService persistScheduledExecutorService;
    /***
     * 时间轮
     */
    private final HashedWheelTimer timer;

    private final ConcurrentMap<Integer /* level */, Long/* offset */> queueOffsetTable =
            new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 当前数据 恢复的偏移量
     */
    private long recoverOffset;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long IMMEDIATE_DELAY = 1L;

    public FixedTimeMessageService(DefaultMessageStore defaultMessageStore){
        this.defaultMessageStore = defaultMessageStore;
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("FixedTimeMessageService-"));
        persistScheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("persistScheduledExecutorService-"));
        timer = new HashedWheelTimer();
        recoverOffset = 0L;
    }

    public void addTask(long queueOffset, int queueId, long delay){
        timer.newTimeout(new FixedTimeTask(queueOffset, queueId), delay, TimeUnit.MILLISECONDS);
    }

    // 单线程将consumerQueue中的消息加载到 时间轮中

    public void start(){
        if (started.compareAndSet(false, true)){
            timer.start();
            FixedTimeMessageService.this.scheduledExecutorService.schedule(new FixedTimeLoadFromConsumeQueueTask(this.recoverOffset),
                    10000L, TimeUnit.MILLISECONDS);
            System.out.println("fixedTimeMessageService started >>>>>>> ");
            persistScheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    FixedTimeMessageService.this.persist();
                }
            }, 30000L, 10000, TimeUnit.MILLISECONDS);
        }
    }

    class FixedTimeLoadFromConsumeQueueTask implements Runnable{
        private long offset;
        FixedTimeLoadFromConsumeQueueTask(long offset){
            this.offset = offset;
        }
        @Override
        public void run() {
            long currentTime = System.currentTimeMillis();
            // 根据偏移量 读取consumerQueue, 将consumerQueue中未消费的数据 加载到时间轮中
            // 默认一个队列
            ConsumeQueue consumeQueue = FixedTimeMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_TIME_TOPIC, 0);
            this.offset = Optional.ofNullable(FixedTimeMessageService.this.queueOffsetTable.get(0)).orElse(0L);
            if (consumeQueue == null){
                FixedTimeMessageService.this.scheduleNextTimerTask(this.offset);
                return;
            }
            // 读取 consumerQueue 队列, 加载到时间轮中
            SelectMappedBufferResult mappedBufferResult = consumeQueue.getIndexBuffer(this.offset);
            if (mappedBufferResult == null){
                FixedTimeMessageService.this.scheduleNextTimerTask(this.offset);
                return;
            }
            int i = 0;
            for (; i < mappedBufferResult.getSize() && isStarted(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE){
                // 从commmitLog加载消息 加载消息具体内容到时间轮
                long offsetPy = mappedBufferResult.getByteBuffer().getLong();
                int sizePy = mappedBufferResult.getByteBuffer().getInt();
                long tagCode = mappedBufferResult.getByteBuffer().getLong();
                MessageExt messageExt = FixedTimeMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
                if (messageExt == null){
                    System.err.println("load message from commitlog error, offsetPy： " + offsetPy + ", sizePy" + sizePy);
                    continue;
                }
                long delayTimeAtTime = messageExt.getDelayTimeAtTime();
                long delayTime =  delayTimeAtTime - currentTime;
                System.out.println("delayTime : " + delayTime + ", delayTimeAtTime: "+ delayTimeAtTime + ", currentTime:" + currentTime);
                if (delayTime <= -1000){
                    continue;
                }
                if (delayTime <= 0){
                    delayTime = 1;
                }
                FixedTimeMessageService.this.addTask(messageExt.getQueueOffset(), messageExt.getQueueId(), delayTime);
            }
            long queueOffset = this.offset + (i / 20);
            // todo 此时只是提交任务到时间轮, 刷新消费进度有问题, 如果此时持久化进度文件,并宕机 则消息会丢。
            FixedTimeMessageService.this.queueOffsetTable.put(0, queueOffset);
            FixedTimeMessageService.this.doImmediate(queueOffset);
        }
    }


    public void doImmediate(long queueOffset){
        FixedTimeMessageService.this.scheduledExecutorService.schedule(new FixedTimeLoadFromConsumeQueueTask(queueOffset), IMMEDIATE_DELAY, TimeUnit.MILLISECONDS);
    }

    private void scheduleNextTimerTask(long offset){
        FixedTimeMessageService.this.scheduledExecutorService.schedule(new FixedTimeLoadFromConsumeQueueTask(offset), DELAY_FOR_A_WHILE, TimeUnit.MILLISECONDS);
    }

    public boolean isStarted(){
        return started.get();
    }

    public class FixedTimeTask implements TimerTask {

        private long queueOffset;
        private int queueId;

        public FixedTimeTask(long queueOffset, int queueId){
            this.queueOffset = queueOffset;
            this.queueId = queueId;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            System.out.println("[FixedTimeTask] execute task >>>> ");
            // 获取 ConsumeQueue
            ConsumeQueue consumeQueue = FixedTimeMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_TIME_TOPIC, queueId);
            if (consumeQueue == null){
                // redo
                System.err.println("[FixedTimeTask] consumer queue is null, queueId: " + queueId);
                return;
            }
            // 获取该offset的 consumerQueue 数据,之后的不用管
            SelectMappedBufferResult bufferResult = consumeQueue.getIndexBuffer(this.queueOffset, ConsumeQueue.CQ_STORE_UNIT_SIZE);
            if (bufferResult == null){
                // redo
                System.err.println("[FixedTimeTask] bufferResult is null, queueOffset: " + queueOffset);
                return;
            }
            // 此时ByteBuffer 会获取很多
            long offsetPy = bufferResult.getByteBuffer().getLong();
            int sizePy = bufferResult.getByteBuffer().getInt();
            // 获取message
            MessageExt msgExt = FixedTimeMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
            if (msgExt == null){
                System.out.println("[FixedTimeTask] load message offsetPy: " + offsetPy + " sizePy:" + sizePy);
                return;
            }
            // 转换消息 topic 换成真实topic
            MessageExtBrokerInner messageExtBrokerInner = FixedTimeMessageService.this.messageTimeup(msgExt);
            // 投递到commitLog
            System.out.println("[MessageExtBrokerInner] topic: " + messageExtBrokerInner.getTopic() + ", queueId: " + messageExtBrokerInner.getQueueId());
            CompletableFuture<PutMessageResult> asyncPutMessage =
                    FixedTimeMessageService.this.defaultMessageStore.asyncPutMessage(messageExtBrokerInner);
            PutMessageResult putMessageResult = asyncPutMessage.get();
            System.out.println("[FixedTimeTask] put message result " + putMessageResult);
            // todo 此时应记录消费的进度。但需要考虑一个问题 比如offset 100 的消息定时时间是2022-05-10 23:00:00
            // 但是 offset 200的消息定时时间是2022-05-10 19:00:00,那么会是offset 200的消息先消费 但此时如果直接将消费进度修改为200,会有问题
            // 可参考 集群并发消费模式的 消费进度上报机制。 treeMap
        }
    }

    /**
     * 转换 消息
     * @param msgExt
     * @return
     */
    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_ARBITRARILY_TIME_LEVEL);
        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);
        return msgInner;
    }

    @Override
    public String encode() {
        return this.encode(true);
    }

    @Override
    public boolean load() {
        // 加载文件
        boolean result = super.load();
        // 校验队列消费进度
        result &= this.correctDelayOffset();
        // 根据消费进度加载定时时间到时间轮
        return result;
    }

    private boolean correctDelayOffset(){
        try{
            for (Integer queueId : this.queueOffsetTable.keySet()) {
                // 根据 topic queueId 获取consumeQueue
                ConsumeQueue consumeQueue = FixedTimeMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_TIME_TOPIC, queueId);
                Long currentDelayOffset = this.queueOffsetTable.get(queueId);
                if (consumeQueue == null || currentDelayOffset == null){
                    continue;
                }
                long correctDelayOffset = currentDelayOffset;
                long cqMinOffset = consumeQueue.getMinOffsetInQueue();
                long cqMaxOffset = consumeQueue.getMaxOffsetInQueue();
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("FixedTime CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                            currentDelayOffset, cqMinOffset, cqMaxOffset, consumeQueue.getQueueId());
                }
                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                            currentDelayOffset, cqMinOffset, cqMaxOffset, consumeQueue.getQueueId());
                }
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ queueId {} ] from {} to {}", queueId, currentDelayOffset, correctDelayOffset);
                    queueOffsetTable.put(queueId, correctDelayOffset);
                }
            }
        }catch (Exception ex){
            return false;
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getFixedTimeDelayOffsetPath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.queueOffsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public synchronized void persist() {
        super.persist();
    }

    @Override
    public String encode(boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.queueOffsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }
}
