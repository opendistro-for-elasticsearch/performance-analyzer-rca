# Building, Installing, and Running the RCA Framework

This document walks you through the process of building and deploying the RCA framework along with the Performance Analyzer plugin. The RCA framework relies on the metrics provided by the [performance analyzer plugin](https://github.com/opendistro-for-elasticsearch/performance-analyzer). Since this code is still in development, the released versions of performance analyzer plugin do not support the RCA framework yet and you will have to build the plugin from source.

    
 ## Building the Performance Analyzer plugin
 
 ### Prerequisites
 Make sure you have the following tools and libraries installed.
 1. git (Mac: `brew install git`)
 2. maven (Mac: `brew install maven`) - Only needed if you are also building the Performance Analyzer plugin.
 3. JDK12+
 
 ### Set up
 1. Create a workspace where you will have all the code checked out
    
    `mkdir workspace`
    
    `cd workspace`
    
 2. Clone the Performance Analyzer RCA repository as follows:
 
    `git clone https://github.com/opendistro-for-elasticsearch/performance-analyzer-rca.git`
    
    `cd performance-analyzer-rca`
    
 3. Set `JAVA_HOME` environment variable to point to JDK12 or higher
 
    `export JAVA_HOME=/path/to/jdk12+`
 
 4. (Optional) IntelliJ setup
 
    a. Launch IntelliJ IDEA
    
    b. Choose Import Project and select the `build.gradle` file in the root of this package
 
### Build RCA framework
This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build.
#### Building from command line
   * `./gradlew build` - Builds, runs unit tests and creates a zip distribution for deployment.
   * The zip distribution can be found under `build/distributions/` folder.
   * You will need to publish the RCA artifact to your maven local repository.
 
   `./gradlew publishToMavenLocal`
        
#### Building from the IDE
   * Currently, the only IDE we support is IntelliJ IDEA. It's free, it's open source, it works. The gradle tasks above can also be launched from IntelliJ's Gradle toolbar.
   * The zip distribution can be found under `build/distributions/` folder.
 
### Build Performance Analyzer Plugin
1. Move back to the `workspace` folder created during setup

2. Clone the Performance Analyzer plugin repository as follows:
 
    `git clone -b master --single-branch https://github.com/opendistro-for-elasticsearch/performance-analyzer.git`
   
3. `cd performance-analyzer`
    
4. Because we are supplying our own version of the RCA framework, the SHA might have changed. So, delete the old SHA file if it exists. The SHA will get updated during build time.
 
    `rm -f licenses/performanceanalyzer-1.3.jar.sha1`

5. Trigger a gradle build. This builds the plugin, runs unit tests and creates the plugin jar.
 
     `./gradlew build`
    
6. The plugin JAR can be found under `build/distributions` folder.
 
## Installing the plugin
 
### Prerequisites

1. Docker - Download and install docker desktop from [Docker website](https://docs.docker.com/docker-for-mac/install/) for Mac.
 
### Setup

Currently, for the alpha development-only source code release, we support installing and running the RCA framework only on Open Distro for Elasticsearch Docker containers.
  
You can use the packaged Dockerfile and docker-compose.yml files [here](./docker) to spin up a cluster with RCA framework installed.
  
1. Create a folder that will hold all the resources that are needed to install and run the RCA framework.
    
   `mkdir rca-infra`

2. `cd rca-infra`

3. Copy all the contents of the docker folder in this repo into our `rca-infra` folder.
    
   `cp <RCA framework root>/docker/* ./`

4. Copy the RCA framework artifact and the Performance Analyzer plugin JAR into this folder
 
    `cp <RCA framework root>/build/distributions/performance-analyzer-rca.zip ./`  
    `cp <Performance Analyzer plugin root>/build/distributions/opendistro_performance_analyzer-1.3.0.0-SNAPSHOT.zip ./`
 
 ### Installation
 
 1. Make sure you're in the `rca-infra` folder.
 
 2. Build and tag the Docker image with our RCA framework.
    
    `docker build  -t odfe-es/pa-rca:1.0  .`
 
 3. Spin up a two node cluster as follows:
 
    `DATA_VOLUME1=esdata1 DATA_VOLUME2=esdata2 docker-compose -f docker-compose.yml -f docker-compose.hostports.yml -f docker-compose.cluster.yml up elasticsearch1 elasticsearch2`
 
 ## Running
 
Once the cluster is up and running, you will need to enable Performance Analyzer and the RCA framework. Both of them are behind feature flags currently.
 
 **NOTE: Performance Analyzer needs to be enabled before the RCA framework is started.**
  
 1. Enable Performance Analyzer
 
    `curl localhost:9200/_opendistro/_performanceanalyzer/cluster/config -H 'Content-Type: application/json' -d '{"enabled": true}' `
 
 2. Enable RCA Framework
 
    `curl localhost:9200/_opendistro/_performanceanalyzer/rca/cluster/config -H 'Content-Type: application/json' -d '{"enabled": true}' `
    
 3. Verify the Settings
    `curl -XGET localhost:9200/_opendistro/_performanceanalyzer/cluster/config`. 
    It should give you `{"currentPerformanceAnalyzerClusterState":3}`
