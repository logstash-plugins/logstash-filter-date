package org.logstash.filters;

import org.joda.time.Instant;
import org.logstash.Event;

import java.io.IOException;

interface ParserExecutor {
  Instant execute(Object input, Event event) throws IOException;
}
