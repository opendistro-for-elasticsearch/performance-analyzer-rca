package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Cluster;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.TestEnvironment;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.TestApi;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;

/**
 * This is the main runner class that is used by the RCA-IT.
 */
public abstract class RcaItRunnerBase extends Runner implements IRcaItRunner {
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

  public RcaItRunnerBase(Class testClass, boolean useHttps) throws Exception {
    super();
    this.testClass = testClass;
    ClusterType clusterType = getClusterTypeFromAnnotation(testClass);
    this.cluster = createCluster(clusterType, useHttps);
    this.testApi = new TestApi(cluster);
    this.testObject = testClass.getDeclaredConstructor().newInstance();

    try {
      Method setClusterMethod = testClass.getMethod(SET_CLUSTER_METHOD, TestApi.class);
      setClusterMethod.setAccessible(true);
      setClusterMethod.invoke(testObject, testApi);
    } catch (NoSuchMethodException ex) {
      // This test class hasn't defined a method setCluster(Cluster). SO probably it does not need
      // access to the cluster object. Which is fine. We move on to the method execution.
    }

    cluster.createServersAndThreads();
    try {
      this.testEnvironment = new TestEnvironment(cluster, testClass);
    } catch (Exception ex) {
      cluster.deleteClusterDir();
      ex.printStackTrace();
      throw ex;
    }
    cluster.startRcaControllerThread();
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
          notifier.fireTestStarted(Description
              .createTestDescription(testClass, method.getName()));

          try {
            this.testEnvironment.updateEnvironment(method);
            this.testEnvironment.verifyEnvironmentSetup();
          } catch (Exception ex) {
            notifier.fireTestFailure(
                new Failure(
                    Description.createTestDescription(testClass.getClass(), method.getName()), ex));
          }
          cluster.startRcaScheduler();

          try {
            method.invoke(testObject);

            if (!validate(method)) {
              notifier.fireTestFailure(
                  new Failure(
                      Description.createTestDescription(testClass.getClass(), method.getName()),
                      new AssertionError()));
            }
          } catch (Exception exception) {
            LOG.error("** ERR: While running method: '{}'", method.getName(), exception);
            notifier.fireTestFailure(
                new Failure(
                    Description.createTestDescription(testClass.getClass(), method.getName()), exception));
          }

          cluster.stopRcaScheduler();
          this.testEnvironment.clearUpMethodLevelEnvOverride();

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
    }
  }

  private boolean validate(Method method)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
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

      boolean allChecksPassed = false;
      while (System.currentTimeMillis() <= endTimeMillis) {
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
              successful = validator.check(testApi.getRcaDataOnHost(expect.on(), rca.getSimpleName()));
              break;
            case RCA_SQLITE:
              successful = validator.check(testApi.getRecordsForAllTables(expect.on(), rca.getSimpleName()));
              break;
          }
          if (successful) {
            passedCount += 1;
          }
        }

        if (passedCount == expectations.length) {
          allChecksPassed = true;
          break;
        }
      }
      return allChecksPassed;
    }
    // The test writer asked for no validations to be performed by the framework. So, we return success.
    return true;
  }
}
