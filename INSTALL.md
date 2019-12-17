# Building, Installing, and Running the RCA Framework

This document walks you through the process of building and deploying the RCA framework along
 with the Performance Analyzer plugin. The RCA framework relies on the metrics provided by the
  [performance analyzer plugin](https://github.com/opendistro-for-elasticsearch/performance-analyzer) 
  and since this is only an alpha release, the released versions of performance analyzer do not
   support the RCA framework. At this point, there are two options.
   1. Use the Performance Analyzer plugin artifact available [here](https://github.com/opendistro-for-elasticsearch/performance-analyzer)
   1. Build Performance Analyzer plugin from source - instructions below.
   
   If you chose to use the packaged artifact you can skip the section - "Build Performance
    Analyzer".
    
 ## Building
 ### Prerequisites
 Make sure you have the following tools and libraries installed.
 1. git (Mac: `brew install git`)
 1. maven (Mac: `brew install maven`) - Only needed if you are also building the Performance
  Analyzer plugin.
 1. JDK12+
 
 ### Setup
 1. Clone this repository
 
    `git clone https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca.git`
 1. Set `JAVA_HOME` environment variable to point to JDK12 or higher
 
    `export JAVA_HOME=/path/to/jdk12+`
 1. (Optional) IntelliJ setup
    1. Launch IntelliJ IDEA
    1. Choose Import Project and select the `build.gradle` file in the root of this package
 
 ### Build RCA framework
 This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build
  system. Gradle comes with excellent documentation that should be your first stop when trying to
   figure out how to operate or modify the build.
   #### Building from command line
   * `./gradlew build` - Builds, runs unit tests and creates a zip distribution for deployment.
   * The zip distribution can be found under `build/distributions/` folder.
   * Skip this step if you're using the pre-built Performance Analyzer plugin JAR. If not, you will
    need to publish the RCA artifact to your maven local repository.
    
        `./gradlew publishToMavenLocal`
   #### Building from the IDE
   * Currently, the only IDE we support is IntelliJ IDEA. It's free, it's open source, it works
   . The gradle tasks above can also be launched from IntelliJ's Gradle toolbar.
   * The zip distribution can be found under `build/distributions/` folder.
 
 ### Build Performance Analyzer Plugin
 1. Clone the Performance Analyzer plugin repository as follows
 
    `git clone -b master --single-branch https://github.com/opendistro-for-elasticsearch/performance-analyzer.git`
 1. Because we are supplying our own version of the RCA framework, the SHA might have changed. So
 , delete the old SHA. The SHA will get updated during build time.
 
    `rm licenses/performanceanalyzer-1.3.jar.sha1`
 1. Trigger a gradle build. This builds the plugin, runs unit tests and creates the plugin jar.
 
    `./gradlew build`
 1. The plugin JAR can be found under `build/distributions` folder.
 
 ## Installing
 ### Prerequisites
 1. docker
 
 ### Setup
 Currently, for the alpha release, we support installing and running the RCA framework on
  OpenDistroForElasticsearch docker containers.
  
 You can use the packaged Dockerfile and docker-compose.yml files to spin up a cluster with RCA
  framework installed.
  
 1. `mkdir rca-infra` Create a folder that will hold all the resources that are needed to install
  and run the RCA framework.
 1. `cd rca-infra`
 1. `cp <RCA framework root>/docker/* ./` Copy all the contents of the docker folder in this repo
  into our `rca-infra` folder.
 1. Copy the RCA framework artifact and the Performance Analyzer plugin JAR into this folder
 
    `cp <RCA framework root>/build/distributions/performance-analyzer-rca.zip ./`  
    `cp <foler where Performance Analyzer plugin was downloaded>/opendistro_performance_analyzer-1.3.0.0-SNAPSHOT.jar`
 
 ### Install
 1. `cd rca-infra` if you're not already in the `rca-infra` folder.
 1. `docker -t odfe-es/pa-rca:1.0 build` Build and tag the docker image with
  our RCA framework.
 1. Spin up a two node cluster as follows:
 
    `DATA_VOLUME1=esdata1 DATA_VOLUME2=esdata2 docker-compose -f docker-compose.yml -f docker-compose.hostports.yml -f docker-compose.cluster.yml up elasticsearch1 elasticsearch2`
 
 ## Running
 Once the cluster is up and running, you will need to enable Performance Analyzer and the RCA
  framework. Both of them are behind feature flags currently.
 
 **NOTE: Performance Analyzer needs to be enabled before the RCA framework can be enabled.**
  
 1. Enable Performance Analyzer
 
    `curl localhost:9200/_opendistro/_performanceanalyzer/cluster/config -H 'Content-Type: application/json' -d '{"enabled": true}' `
 1. Enable RCA Framework
 
    `curl localhost:9200/_opendistro/_performanceanalyzer/cluster/config -H 'Content-Type: application/json' -d '{"enabled": true}' `