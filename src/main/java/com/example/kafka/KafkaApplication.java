package com.example.kafka;

import com.example.kafka.listener.KafkaConsumerListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);

        KafkaConsumerListener kafkaConsumerHandler = context.getBean(KafkaConsumerListener.class);
        kafkaConsumerHandler.runConsumer();

    }

}
