package id.k.vertxkafka;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

/**
 * created by ksadewo on 03/06/20 - 20.45
 */
public class MainVerticle extends AbstractVerticle {

    @Override
    public void start() {
        vertx.deployVerticle(ProducerVerticle.class.getName());
        vertx.deployVerticle(ConsumerVerticle.class.getName());
    }

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new MainVerticle());
    }
}
