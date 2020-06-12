
package com.example.demo;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.RoutingKafkaTemplate;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Configuration
public class Config {

    public static final String TOPIC1 = "Topic1";
    public static final String TOPIC2 = "Topic2";

    @Configuration
    @ConfigurationProperties("kafka")
    public static class KafkaProps {
        private String servers;

        public String getServers() {
            return servers;
        }

        public void setServers(String servers) {
            this.servers = servers;
        }
    }

    @Bean
    public RoutingKafkaTemplate routingTemplate(KafkaProps props) {
       ensureTopics(props);

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,30000);

        ProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(configProps);

        Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("Topic2"), factory);
        map.put(Pattern.compile(".+"), factory);

        return new RoutingKafkaTemplate(map);
    }

    private void ensureTopics(KafkaProps props) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getServers());

        try (AdminClient client = AdminClient.create(configs)) {
            Set<String> topics = new HashSet<>(Arrays.asList(TOPIC1, TOPIC2));
            DescribeTopicsResult result = client.describeTopics(topics);
            topics.removeAll(getExisting(result.values()));

            if (!topics.isEmpty()) {
                List<NewTopic> newTopics = topics.stream().map(t -> new NewTopic(t, 21, (short)1)).collect(Collectors.toList());
                client.createTopics(newTopics).all().get();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> getExisting(Map<String, KafkaFuture<TopicDescription>> values) {
        Set<String> result = new HashSet<>();
        values.values().forEach(future -> {
            try {
                result.add(future.get().name());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
            }
        });
        return result;
    }
}

