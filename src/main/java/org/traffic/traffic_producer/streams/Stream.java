package org.traffic.traffic_producer.streams;

import io.vertx.core.json.JsonObject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Stream {

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");
  String id;
  Double latitude;
  Double longitude;
  LocalDateTime streamStart;
  String sensorId;
  String feature;

  public Stream(
      Double latitude,
      Double longitude,
      LocalDateTime streamStart,
      String sensorId,
      String feature) {

    this.id = UUID.randomUUID().toString() + "-" + feature;
    this.latitude = latitude;
    this.longitude = longitude;
    this.streamStart = streamStart;
    this.sensorId = sensorId;
    this.feature = feature;
  }

  public static Stream fromJson(JsonObject json) {
    return new Stream(
        json.getString("id"),
        json.getDouble("latitude"),
        json.getDouble("longitude"),
        LocalDateTime.parse(json.getString("streamStart"), DATE_TIME_FORMATTER),
        json.getString("sensorId"),
        json.getString("feature"));
  }

  public static JsonObject asJson(Stream stream) {
    return new JsonObject()
        .put("id", stream.id)
        .put("latitude", stream.latitude)
        .put("longitude", stream.longitude)
        .put("streamStart", stream.streamStart.format(DATE_TIME_FORMATTER))
        .put("sensorId", stream.sensorId)
        .put("feature", stream.feature);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Stream stream = (Stream) o;
    return id.equals(stream.id)
        && latitude.equals(stream.latitude)
        && longitude.equals(stream.longitude)
        && streamStart
            .truncatedTo(ChronoUnit.MINUTES)
            .isEqual(stream.streamStart.truncatedTo(ChronoUnit.MINUTES))
        && sensorId.equals(stream.sensorId)
        && feature.equals(stream.feature);
  }
}
