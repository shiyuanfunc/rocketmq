#### 单模块打包
```json
  mvn -U -pl client clean package -DskipTests -Dspotbugs.skip=true -Dcheckstyle.skip=true
```
#### consumer 限流
* 由于consumer是以topic的MessageQueue为基础拉取消息的，可考虑控制topic MessageQueue拉取消息的频率
        但此方式需注意 一次消息拉取是多条消息,默认是32条,虽控制了拉取消息的频率,但是拉取后还是并发消费,此种方式瞬时并发度并不会降低. 可控制一个时间段内的总消费数（不获取新消息）。
* 由于consumer 拉取到消息后是以org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest任务的形式提交到线程池中,可通过控制该任务的执行频率控制并发
  * 任务执行时判断 当前group + topic是否可执行。不可执行时再次提交到线程池中。
    * 判断当前任务是否可执行时,需要以group + topic 以区分同一topic，不同分组订阅的情况,此时并发并不一定相同
    * 此种方式会造成ProcessQueue中会挤压消息
      * 比如两次拉取中，分别拉取到offset为1 和offset为1500的两条消息, 此方式会导致1500已经被处理了但是0的持久未处理,需注意幂等。
        * 由于consumer上报当前MessageQueue的消费位点时,始终取的ProcessQueue中的最小值（offset 1500被消费后，上报位点取的是pq移除该消息后的最小offset,因此此时当前队列的消费位点还是1）, 因此此时consumer重启后从Broker端获取到当前MessageQueue的消费位点还是1, 1500的数据会被重复消费
      * 此种方式的挤压消息增大时,会自动阻断consumer 当前MessageQueue队列的消息拉取,因为 ProcessQueue的span(lastKey - firstKey, 当前pq中最大的offstet - 最小的offset)差距过大,默认值2000