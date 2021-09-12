package org.traffic.traffic_producer;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.traffic.traffic_producer.observations.ObservationService;
import org.traffic.traffic_producer.sensors.SensorService;
import org.traffic.traffic_producer.streams.StreamService;

@Slf4j
public final class MainVerticle extends AbstractVerticle {

  private JsonObject config;

  public static void main(String[] args) {
    Launcher.executeCommand("run", MainVerticle.class.getName());
  }

  @Override
  public void start(Promise<Void> startPromise) {

    val configRetriever = ConfigRetriever.create(vertx);

    log.info("Loading configuration");
    // Deploying services

    configRetriever
        .getConfig()
        .map(
            config -> {
              log.info("Configuration loaded");
              return this.config = config;
            })
        .compose(
            __ ->
                vertx.deployVerticle(
                    new SensorService(),
                    new DeploymentOptions()
                        .setWorker(true)
                        .setConfig(config.getJsonObject("sensor-service"))))
        .compose(
            __ ->
                vertx.deployVerticle(
                    new StreamService(),
                    new DeploymentOptions()
                        .setWorker(true)
                        .setConfig(config.getJsonObject("stream-service"))))
        .compose(
            __ ->
                vertx.deployVerticle(
                    new ObservationService(),
                    new DeploymentOptions()
                        .setWorker(true)
                        .setConfig(config.getJsonObject("observation-service"))))
        .compose(
            __ ->
                vertx.deployVerticle(
                    new IoTServiceGatewayVerticle(),
                    new DeploymentOptions()
                        .setWorker(true)
                        .setConfig(config.getJsonObject("service-gateway"))))
        .onSuccess(__ -> startPromise.complete())
        .onFailure(startPromise::fail);
  }
}
