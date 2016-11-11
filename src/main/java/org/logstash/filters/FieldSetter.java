package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;
import org.logstash.Timestamp;

class FieldSetter implements ResultSetter {
  private String target;

  FieldSetter(String target) {
    this.target = target;
  }

  public void set(Event event, Instant instant) {
    event.setField(this.target, new Timestamp(instant.getMillis()));
  }
}
