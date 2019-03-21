package com.example.kafka.service;

import com.example.kafka.handler.KafkaProducerHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ProducerService extends KafkaProducerHandler {

    public String sendKafka() throws Exception{
        try {
            runProducer("example-log",1);
        }catch (Exception e){
            e.printStackTrace();
            return "fail";
        }
        return "success";
    }




}
