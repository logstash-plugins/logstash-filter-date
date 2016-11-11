package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.Timestamp;

class TimestampSetter implements ResultSetter {
  public void set(Event event, Instant instant) {
    event.setTimestamp(new Timestamp(instant.getMillis()));
  }
}
