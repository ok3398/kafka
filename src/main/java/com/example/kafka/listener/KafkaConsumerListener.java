package com.example.kafka.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.kafka.util.PooledHttpClient;
import com.example.kafka.vo.PooledVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Component
public class KafkaConsumerListener {

    @Value("${kafka.server.ip}")
    private String KAFKA_SERVERS;

    private  Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "consumerGroupExample");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put((ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), false);

        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                new KafkaConsumer<Long, String>(props);

        // Subscribe to the topic.
        consumer.subscribe(Arrays.asList("example-log"));

        return consumer;
    }
    public void runConsumer() throws Exception {
        final Consumer<Long, String> consumer = createConsumer();

        try {
            while (true) {
                final ConsumerRecords<Long, String> consumerRecords = consumer.poll(100);
                PooledVo pooledVo = new PooledVo();
                for (ConsumerRecord<Long, String> record : consumerRecords) {
                    log.info("consumer debug > key: [" + record.key() + "] ,value: [" +record.value() + "] , partition: [" +record.partition() + "], offset:["+ record.offset()+"]");
                    pooledVo = insertLog(record);
                }
//                consumerRecords.forEach(record -> {
//                    log.info("consumer debug > key: [" + record.key() + "] ,value: [" +record.value() + "] , partition: [" +record.partition() + "], offset:["+ record.offset()+"]");
//                });

                try {
                    if(pooledVo.getStatusCode() == 200)
                        consumer.commitSync();
                }catch (CommitFailedException e){
                    e.printStackTrace();
                }

            }
        }finally {
            consumer.close();

        }
    }

    private PooledVo insertLog(ConsumerRecord<Long, String> record) {
        try {
            JSONParser parser = new JSONParser();
            JSONObject jsonObj = (JSONObject) parser.parse(record.value());
            ObjectMapper objectMapper = new ObjectMapper();

            Map<String, Object> logParam =  objectMapper.readValue(jsonObj.toJSONString(), new TypeReference<Map<String,Object>>() {});

            PooledHttpClient client = new PooledHttpClient("test.co.kr/test","");
            Object key = null;
            Iterator it = logParam.keySet().iterator();
            while (it.hasNext()){
                key = it.next();
                client.addParam((String)key, MapUtils.getString(logParam, key, ""));
            }

//            JSONParser resultParser = new JSONParser();
//            JSONObject resultObj = (JSONObject)resultParser.parse(result);
            return client.POST();
        }catch (Exception e){
            e.printStackTrace();
        }
        return new PooledVo();
    }
}
