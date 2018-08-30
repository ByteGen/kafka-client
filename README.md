# kafka client
Simple kafka client tool. 

## Usage
1. 添加 pom 依赖
```
<dependency>
    <groupId>com.bytegen.common</groupId>
    <artifactId>kafka-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```
2. 设置 KafkaTopic

- producer

IKafkaProducer 接口定义消息发送模式, 提供了 ThreadPool 模式和 Synchronize 模式两种 produce 模式.

- consumer

IKafkaConsumer 接口定义消息消费模式, 提供了 AutoCommit 模式(拉取到本地队列后通过线程池处理)和 ManualCommit 模式(拉取消息后同步处理并发送ACK)两种 consume 模式.

IConsumerWorker 接口定义对消息的具体的处理逻辑, 示例如下:
```java
public class LoggingConsumerWorker implements IConsumerWorker {
    private static final Logger logger = LoggerFactory.getLogger(LoggingConsumerWorker.class);

    @Override
    public void consume(byte[] msg) {
        if (null != msg) {
            logger.info("Consume kafka message: " + new String(msg));
        } else {
            logger.warn("Receive NULL kafka message!");
        }
    }

}
```
3. 自定义monitor

实现 KafkaEventListener 接口; 并在 KafkaEventMonitor 进行注册. 参考如下示例:
```java
public class LoggingEventListener implements KafkaEventListener {
       
    @Override
    public void onAction(String traceId, KafkaEventType type, String topicName,
                        String message, Object value) {
       logger.info("{\"trace_id\":\"" + traceId + "\",\"type\":\"" + type + "\",\"topic\":\"" + topicName
               + "\",\"message\":\"" + message + "\",\"value\":\"" + value + "\"}");
    }
    
    static {
        KafkaEventMonitor.getInstance().addSubscriber(new LoggingEventListener());
    }
}
```
