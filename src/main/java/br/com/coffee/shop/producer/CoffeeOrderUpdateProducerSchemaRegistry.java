package br.com.coffee.shop.producer;

import br.com.coffee.shop.generated.CoffeeUpdateEvent;
import br.com.coffee.shop.generated.OrderStatus;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CoffeeOrderUpdateProducerSchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(CoffeeOrderUpdateProducerSchemaRegistry.class);
    private static final String COFFEE_ORDERS_TOPIC = "coffee-orders-sr";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        KafkaProducer<String, CoffeeUpdateEvent> producer = new KafkaProducer<>(properties);

        CoffeeUpdateEvent coffeeOrderUpdateEvent = buildCoffeeOrderUpdateEvent();

        ProducerRecord<String, CoffeeUpdateEvent> producerRecord =
                new ProducerRecord<>(COFFEE_ORDERS_TOPIC, coffeeOrderUpdateEvent.getId().toString(),
                        coffeeOrderUpdateEvent);
        var recordMetaData = producer.send(producerRecord).get();

        log.info("recordMetaData : " + recordMetaData);

    }

    private static CoffeeUpdateEvent buildCoffeeOrderUpdateEvent() {

        return CoffeeUpdateEvent.newBuilder()
                .setId(UUID.fromString("907454d0-e992-4aac-bef1-c621f15e5d7a"))
                .setStatus(OrderStatus.PROCESSING)
                .build();

    }

}