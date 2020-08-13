package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.log;

import java.util.Collection;
import java.util.Objects;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.junit.Assert;

public class AppenderHelper {
  public static Configuration addMemoryAppenderToRootLogger() {
    Configuration oldConfiguration = LoggerContext.getContext().getConfiguration();

    ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
    builder.setStatusLevel(Level.INFO);
    builder.setConfigurationName("RcaItLogger");
    builder.setPackages("com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.log");
    RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.INFO);

    addRcaItInMemoryAppender(builder, rootLogger);
    addConsoleAppender(builder, rootLogger);

    builder.add(rootLogger);
    Configuration configuration = builder.build();
    Configurator.reconfigure(configuration);
    return oldConfiguration;
  }

  private static void addRcaItInMemoryAppender(ConfigurationBuilder builder, RootLoggerComponentBuilder rootLogger) {
    AppenderComponentBuilder appenderBuilder = builder.newAppender(RcaItInMemoryAppender.NAME, RcaItInMemoryAppender.NAME);
    addLogPattern(builder, appenderBuilder);
    rootLogger.add(builder.newAppenderRef(RcaItInMemoryAppender.NAME));
    builder.add(appenderBuilder);

  }

  private static void addConsoleAppender(ConfigurationBuilder builder, RootLoggerComponentBuilder rootLogger) {
    AppenderComponentBuilder appenderBuilder = builder.newAppender("Console", "CONSOLE").addAttribute("target",
        ConsoleAppender.Target.SYSTEM_OUT);
    addLogPattern(builder, appenderBuilder);
    rootLogger.add(builder.newAppenderRef("Console"));
    builder.add(appenderBuilder);
  }

  private static void addLogPattern(ConfigurationBuilder builder, AppenderComponentBuilder appenderBuilder) {
    appenderBuilder.add(
        builder.newLayout("PatternLayout").addAttribute("pattern", RcaItInMemoryAppender.PATTERN));
  }

  public static void setLoggerConfiguration(Configuration configuration) {
    Objects.requireNonNull(configuration);
    Configurator.reconfigure(configuration);
  }

  public static void verifyNoErrorLogs() throws IllegalStateException {
    Collection<String> errors = RcaItInMemoryAppender.self().getAllErrors();
    if (errors.size() > 0) {
      StringBuilder err = new StringBuilder(
          "RCA-IT fails if there are any errors that were logged. The Runner found the following: [");
      for (String error : errors) {
        err.append(System.lineSeparator()).append(error);
      }
      err.append(System.lineSeparator()).append("]");
      throw new IllegalStateException(err.toString());
    }
  }

  public static void resetErrors() {
    RcaItInMemoryAppender.self().reset();
  }
}
