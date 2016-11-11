package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;

import java.io.IOException;

interface InputHandler {
  Instant handle(String input, Event event) throws IOException;
}
