package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.log;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

@Plugin(name = RcaItInMemoryAppender.NAME, category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class RcaItInMemoryAppender extends AbstractAppender {
  public static final String NAME = "RcaItInMemory";
  public static final String PATTERN = "%d{HH:mm:ss.SSS} [%t] %-5level %logger{1}:%M()::line:%L - %msg%n";

  private static volatile RcaItInMemoryAppender self;

  private final ArrayBlockingQueue<String> errorLogs = new ArrayBlockingQueue<>(1000);


  public RcaItInMemoryAppender() {
    super(NAME, null, null, true, Property.EMPTY_ARRAY);
  }

  private RcaItInMemoryAppender(final String name, final Layout<?> layout, final Property[] properties) {
    super(name, null, layout, true, properties);
  }


  /**
   * Creates a CountingNoOp Appender.
   */
  @PluginFactory
  public static RcaItInMemoryAppender createAppender(@PluginAttribute("name") final String name,
                                                     @PluginElement("Layout") Layout layout) {
    self = new RcaItInMemoryAppender(Objects.requireNonNull(name), layout, Property.EMPTY_ARRAY);
    return self;
  }

  public static RcaItInMemoryAppender self() {
    return self;
  }

  @Override
  public void append(LogEvent event) {
    if (event.getLevel() == Level.ERROR) {
      try {
        String message = new String(getLayout().toByteArray(event), StandardCharsets.UTF_8);
        if (!errorLogs.add(message)) {
          System.out.println("Could not write to queue.");
        }
      } catch (IllegalStateException ex) {
        System.out.println("Could not write to error Queue.");
      }
    }
  }

  public void reset() {
    errorLogs.clear();
  }

  public Collection<String> getAllErrors() {
    Collection<String> errors = new ArrayList<>();
    errorLogs.drainTo(errors);
    return errors;
  }
}
