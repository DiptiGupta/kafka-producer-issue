# kafka-producer-issue

##How to reproduce
1. Run Kafka in docker container (e.g. wurstmeister/kafka:latest)
2. Set kafka.servers to <host:port> of Kafka bootstrap servers in application.properties
3. Build and run Spring application
4. Invoke /send method of REST API (e.g. curl -X GET http://localhost:9008/send

Expected behavior:
- two messages are sent to two topics

Actual behavior:
- only one message is sent to one topic
- for another message exception is seen: TimeoutException: Failed to fetch metadata after <specified_max_block> ms

Note: It works fine if /sendToTopic2 endpoint is invoked first.