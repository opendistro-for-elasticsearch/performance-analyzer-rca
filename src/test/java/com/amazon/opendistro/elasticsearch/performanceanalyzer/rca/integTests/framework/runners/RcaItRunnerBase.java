package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Cluster;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.RcaItMarker;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.TestEnvironment;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AErrorPatternIgnored;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.TestApi;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.log.AppenderHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryRcaRequestHandler;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.Filterable;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;

/**
 * This is the main runner class that is used by the RCA-IT.
 */
public abstract class RcaItRunnerBase extends Runner implements IRcaItRunner, Filterable {
  private static final Logger LOG = LogManager.getLogger(RcaItRunnerBase.class);

  // The class whose tests the runner is currently executing.
  private final Class testClass;

  // An instance of the test class the runner is executing.
  private final Object testObject;

  // This is used to set up the environment. An environment for running RCA graph would be to push the RCA graph itself,
  // the metrics, the rca.conf if that needs to be changed. It reads them from the annotations and sets them up for the
  // cluster object.
  private final TestEnvironment testEnvironment;

  // An instance of the cluster where tests are running.
  private final Cluster cluster;

  // This is wrapper on top of the cluster object that is passed on to the testClass to get access to the cluster.
  private final TestApi testApi;

  private final Configuration oldConfiguration;

  public RcaItRunnerBase(Class testClass, boolean useHttps) throws Exception {
    super();

    checkTestClassMarked(testClass);

    this.oldConfiguration = AppenderHelper.addMemoryAppenderToRootLogger();
    this.testClass = testClass;
    ClusterType clusterType = getClusterTypeFromAnnotation(testClass);
    this.cluster = createCluster(clusterType, useHttps);
    this.testApi = new TestApi(cluster);
    this.testObject = testClass.getDeclaredConstructor().newInstance();

    setTestApiForTestClass();

    cluster.createServersAndThreads();
    try {
      this.testEnvironment = new TestEnvironment(cluster, testClass);
    } catch (Exception ex) {
      cluster.deleteClusterDir();
      ex.printStackTrace();
      AppenderHelper.setLoggerConfiguration(oldConfiguration);
      throw ex;
    }
    cluster.startRcaControllerThread();
  }

  private static void checkTestClassMarked(Class testClass) {
    Category categoryAnnotation = (Category) testClass.getAnnotation(Category.class);
    Objects.requireNonNull(
        categoryAnnotation,
        "All RcaIt test classes must have annotation '@Category(RcaItMarker.class). "
            + "Not found for class: " + testClass.getName());
    Assert.assertEquals("The number of expected annotation value is 1.", 1, categoryAnnotation.value().length);
    Assert.assertEquals(RcaItMarker.class, categoryAnnotation.value()[0]);
  }

  private void setTestApiForTestClass() {
    try {
      Method setClusterMethod = testClass.getMethod(SET_CLUSTER_METHOD, TestApi.class);
      setClusterMethod.setAccessible(true);
      setClusterMethod.invoke(testObject, testApi);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
      // This test class hasn't defined a method setCluster(Cluster). SO probably it does not need
      // access to the cluster object. Which is fine. We move on to the method execution.
    }
  }

  private static ClusterType getClusterTypeFromAnnotation(Class testClass) {
    if (!testClass.isAnnotationPresent(AClusterType.class)) {
      throw new IllegalArgumentException(
          testClass.getSimpleName() + " does not have the mandatory annotation: " + AClusterType.class.getSimpleName());
    }
    return ((AClusterType) testClass.getAnnotation(AClusterType.class)).value();
  }

  @Override
  public Description getDescription() {
    return Description.createTestDescription(testClass, "A custom runner for RcaIt");
  }

  @Override
  public void run(RunNotifier notifier) {
    try {
      for (Method method : testClass.getMethods()) {
        if (method.isAnnotationPresent(Test.class)) {
          notifier.fireTestStarted(Description.createTestDescription(testClass, method.getName()));

          try {
            prepareForRun(method);
            method.invoke(testObject);
            validateTestRun(method);
          } catch (Exception exception) {
            notifier.fireTestFailure(
                new Failure(
                    Description.createTestDescription(testClass.getClass(), method.getName()), exception));
          }

          postRunCleanups();
          notifier.fireTestFinished(Description.createTestDescription(testClass, method.getName()));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } finally {
      try {
        cluster.deleteCluster();
      } catch (IOException e) {
        e.printStackTrace();
      }
      AppenderHelper.setLoggerConfiguration(oldConfiguration);
    }
  }

  private void postRunCleanups() {
    try {
      cluster.stopRcaScheduler();
    } catch (Exception e) {
      e.printStackTrace();
    }
    this.testEnvironment.clearUpMethodLevelEnvOverride();
  }

  private void validateTestRun(Method method)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalStateException {
    List<Class> failedChecks = validateTestOutput(method);

    if (!failedChecks.isEmpty()) {
      StringBuilder sb = new StringBuilder("Failed validations for:");
      for (Class failed : failedChecks) {
        sb.append(System.lineSeparator()).append(failed);
      }
      throw new IllegalStateException(sb.toString());
    }
    validateNoErrorsInLog(method);
  }

  private void validateNoErrorsInLog(Method method) {
    if (method.isAnnotationPresent(AErrorPatternIgnored.class) || method.isAnnotationPresent(AErrorPatternIgnored.Patterns.class)) {
      Set<String> patternsToIgnore =
          Arrays.stream(method.getAnnotationsByType(AErrorPatternIgnored.class)).map(AErrorPatternIgnored::pattern)
              .collect(Collectors.toSet());

      Collection<String> errors = AppenderHelper.getAllErrorsInLog();
      if (errors.size() > 0) {
        // For each of the errors, we check if they have the pattern that we are supposed to ignore. If so, we ignore them.
        List<String> fatalErrors =
            errors
                .stream()
                .filter(
                    error -> patternsToIgnore
                        .stream()
                        .noneMatch(
                            pattern -> error.contains(pattern)
                        )
                ).collect(Collectors.toList());

        if (!fatalErrors.isEmpty()) {

          StringBuilder err = new StringBuilder(
              "RCA-IT fails if some errors are found in the logs. If you think these errors are okay to ignore, you can use the "
                  + "@AErrorPatternIgnored to ignore them. Please see RcaItPocSingleNode.simple() for an example.");
          err.append(System.lineSeparator()).append("The Runner found the following errors in log: [");
          err.append(System.lineSeparator());

          fatalErrors.forEach(x -> err.append(x));
          err.append(System.lineSeparator()).append("]");
          throw new IllegalStateException(err.toString());
        }
      }
    }
  }

  private void prepareForRun(Method method) throws Exception {
    applyMethodLevelAnnotationOverrides(method);
    cluster.startRcaScheduler();
    AppenderHelper.resetErrors();
  }

  private void applyMethodLevelAnnotationOverrides(Method method) throws Exception {
    this.testEnvironment.updateEnvironment(method);
    this.testEnvironment.verifyEnvironmentSetup();
  }

  private List<Class> validateTestOutput(Method method)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    List<Class> failedValidations = new ArrayList<>();
    if (method.isAnnotationPresent(AExpect.Expectations.class) || method.isAnnotationPresent(AExpect.class)) {
      AExpect[] expectations = method.getDeclaredAnnotationsByType(AExpect.class);

      IValidator[] validators = new IValidator[expectations.length];
      long maxWaitMillis = 0;

      // Initialization loop for validation and the maximum wait time for the checks to pass.
      for (int i = 0; i < expectations.length; i++) {
        AExpect expect = expectations[i];
        validators[i] = (IValidator) expect.validator().getDeclaredConstructor().newInstance();
        long timeOutMillis = TimeUnit.MILLISECONDS.convert(expect.timeoutSeconds(), TimeUnit.SECONDS);
        if (timeOutMillis > maxWaitMillis) {
          maxWaitMillis = timeOutMillis;
        }
      }

      long startMillis = System.currentTimeMillis();
      long endTimeMillis = startMillis + maxWaitMillis;


      while (System.currentTimeMillis() <= endTimeMillis) {
        failedValidations.clear();
        int passedCount = 0;
        // All checks must pass for one run for the validations to succeed. It's not valid if
        // different checks pass for different runs.
        for (int i = 0; i < expectations.length; i++) {
          // This is already initialized. Cannot be null.
          IValidator validator = validators[i];
          AExpect expect = expectations[i];
          AExpect.Type what = expect.what();
          boolean successful = false;

          Class rca = expect.forRca();

          switch (what) {
            case REST_API:
              Map<String, String> params = new HashMap<>();
              if (rca == PersistedAction.class) {
                successful = validator.checkJsonResp(
                    testApi.getRestResponse(Util.ACTIONS_QUERY_URL, params, expect.on()));
              }
              else {
                //TODO: we should read RCA output directly from rest endpoint if what = REST_API
                // the current getRcaDataOnHost read data from sql DB file
                successful = validator.checkJsonResp(testApi.getRcaDataOnHost(expect.on(), rca.getSimpleName()));
              }
              break;
            case DB_QUERY:
              try {
                successful = validator.checkDbObj(testApi.constructObjectFromDBOnHost(expect.on(), rca));
              } catch (Exception e) {
                // if any exceptions occur on reading from the DB, we want to continue.
                // exceptions might also mean the data is not yet available in the DB.
                // successful will remain false.
              }
              break;
          }
          if (successful) {
            passedCount += 1;
          } else {
            failedValidations.add(validator.getClass());
          }
        }

        if (passedCount == expectations.length) {
          break;
        }
      }
    }
    return failedValidations;
  }

  @Override
  public void filter(Filter filter) {

  }
}
