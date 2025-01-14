# KafkaBlogs

command to create topic with 3mb message limit
```bash
./kafka-topics.sh --bootstrap-server 127.0.0.1:9092  --create --topic bigMsgTopic --config max.message.bytes=3024822
```


Kakfa Broker Configuration for 3 mb size

```bash
message.max.bytes=3024822
replica.fetch.max.bytes=3024822
```