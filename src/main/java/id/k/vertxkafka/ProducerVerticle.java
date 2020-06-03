package id.k.vertxkafka;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import io.vertx.kafka.client.producer.KafkaWriteStream;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * created by ksadewo on 03/06/20 - 20.51
 */
public class ProducerVerticle extends AbstractVerticle {

    KafkaWriteStream<String, JsonObject> producer;

    private final Logger LOGGER = LoggerFactory.getLogger(ProducerVerticle.class);

    @Override
    public void start() throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class.getName());

        producer = KafkaWriteStream.create(vertx, properties, String.class, JsonObject.class);

        vertx.setPeriodic(1000, event -> {
            JsonObject object = new JsonObject();
            object.put("name", UUID.randomUUID().toString());
            object.put("time", System.currentTimeMillis());

            WriteStream<ProducerRecord<String, JsonObject>> stream = producer.write(new ProducerRecord<>("my_topic", "key", object));
            stream.exceptionHandler(event1 -> LOGGER.error(event1.getMessage()));
        });
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }

}
