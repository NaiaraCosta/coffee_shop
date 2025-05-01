package br.com.coffee.shop.consumer;

import br.com.coffee.shop.generated.CoffeeOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.Collections;
import java.util.Properties;

public class CoffeeOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(CoffeeOrderConsumer.class);
    private static final String COFFEE_ORDERS_TOPIC = "coffee-orders";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "coffee.consumer");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(properties);
        consumer.subscribe(Collections.singletonList(COFFEE_ORDERS_TOPIC));

        log.info("Consumer Started");

        while(true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, byte[]> record : records) {
                try {
                    CoffeeOrder coffeeOrder = decodeCoffeeOrder(record.value());

                    log.info("Consumed message, key: {},  value: {}", record.key(), coffeeOrder.toString());

                    var utcDateTime = LocalDateTime.ofInstant(coffeeOrder.getOrderTime(), ZoneOffset.UTC);

                    var cstDateTime = LocalDateTime.ofInstant(coffeeOrder.getOrderTime(), ZoneId.of("America/Sao_Paulo"));

                    log.info("utcDateTime: {},  cstDateTime: {}", utcDateTime, cstDateTime);

                } catch (Exception e) {
                    log.error("Exception is: {}", e.getMessage(), e);
                }
            }
        }
    }

    public static CoffeeOrder decodeCoffeeOrder(byte[] array) throws IOException {
        return CoffeeOrder.fromByteBuffer(ByteBuffer.wrap(array));
    }

}