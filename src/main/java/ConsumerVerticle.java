import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaReadStream;
import io.vertx.kafka.client.serialization.JsonObjectDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.Properties;

/**
 * created by ksadewo on 03/06/20 - 20.52
 */
public class ConsumerVerticle extends AbstractVerticle {

    private final Logger LOGGER = LoggerFactory.getLogger(ConsumerVerticle.class);

    KafkaReadStream<String, JsonObject> consumer;

    @Override
    public void start() throws Exception {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonObjectDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");

        consumer = KafkaReadStream.create(vertx, properties, String.class, JsonObject.class);
        consumer.subscribe(Collections.singleton("my_topic"));

        consumer.handler(event -> {
            JsonObject object = event.value();

            LOGGER.info("Hulla " + object.getString("name") + " @" + new Date(object.getLong("time")));
        });

    }

    @Override
    public void stop() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
