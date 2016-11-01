package org.logstash.filters.parser;

import org.joda.time.Instant;

public class UnixMillisEpochParser implements TimestampParser {

  @Override
  public Instant parse(String value) {
    return parse(Long.parseLong(value));
  }

  @Override
  public Instant parse(Long value) {
    return new Instant(value);
  }

  @Override
  public Instant parse(Double value) {
    // XXX: Should we accept a double?
    return parse(value.longValue());
  }

  @Override
  public Instant parseWithTimeZone(String value, String timezone) {
    return parse(value);
  }
}
