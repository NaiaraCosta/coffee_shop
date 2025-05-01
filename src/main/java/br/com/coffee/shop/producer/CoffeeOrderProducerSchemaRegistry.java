package br.com.coffee.shop.producer;

import br.com.coffee.shop.generated.CoffeeOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static br.com.coffee.shop.util.CoffeeOrderUtil.buildNewCoffeeOrder;

public class CoffeeOrderProducerSchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(CoffeeOrderProducerSchemaRegistry.class);
    private static final String COFFEE_ORDERS_TOPIC = "coffee-orders-sr";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // properties.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
        properties.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());

        KafkaProducer<String, CoffeeOrder> producer = new KafkaProducer<String, CoffeeOrder>(properties);

        CoffeeOrder coffeeOrder = buildNewCoffeeOrder();

        log.info("Coffee order sent " + coffeeOrder.toString());

        // ProducerRecord<OrderId, CoffeeOrder> producerRecord = new ProducerRecord<>(COFFEE_ORDERS_TOPIC, coffeeOrder.getId(), coffeeOrder);
        ProducerRecord<String, CoffeeOrder> producerRecord = new ProducerRecord<>(COFFEE_ORDERS_TOPIC, coffeeOrder.getId().toString(), coffeeOrder);

        var recordMetaData = producer.send(producerRecord).get();

        log.info("recordMetaData : {}", recordMetaData);

        log.info("Published the producer record : {}", producerRecord);

    }

}