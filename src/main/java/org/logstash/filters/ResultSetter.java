package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;

interface ResultSetter {
  void set(Event event, Instant instant);
}
