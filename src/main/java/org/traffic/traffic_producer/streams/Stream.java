package org.traffic.traffic_producer.streams;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.traffic.traffic_producer.point.Point;
import org.traffic.traffic_producer.sensors.Sensor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Stream {

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");

  String id;
  LocalDateTime streamStart;
  String sensorId;
  String feature;
  Point location;

  public Stream(Sensor sensor) {
    this.id = UUID.randomUUID() + "-" + feature;
    this.streamStart = LocalDateTime.now();
    this.sensorId = sensor.getId();
    this.feature = sensor.getQuantityKind();
    this.location = new Point(sensor.getLatitude(), sensor.getLongitude());
  }

  public static Stream fromJson(JsonObject json) {
    return new Stream(
        json.getString("id"),
        LocalDateTime.parse(json.getString("streamStart"), DATE_TIME_FORMATTER),
        json.getString("sensorId"),
        json.getString("feature"),
        Point.fromJson(json.getJsonObject("location")));
  }

  public static JsonObject asJson(Stream stream) {
    return new JsonObject()
        .put("id", stream.id)
        .put("location", Point.asJson(stream.location))
        .put("streamStart", stream.streamStart.format(DATE_TIME_FORMATTER))
        .put("sensorId", stream.sensorId)
        .put("feature", stream.feature);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Stream stream = (Stream) o;
    return id.equals(stream.id)
        && streamStart
            .truncatedTo(ChronoUnit.MINUTES)
            .isEqual(stream.streamStart.truncatedTo(ChronoUnit.MINUTES))
        && sensorId.equals(stream.sensorId)
        && feature.equals(stream.feature)
        && location.equals(stream.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, streamStart, sensorId, feature, location);
  }
}
