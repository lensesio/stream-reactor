# Helm Chart and Mesos Universe Generator

Generates Helm Charts for Kubernetes and Packages for Mesos (Pending). It scans the classpath for SinkConnectors and SourceConnectors
and generates the templates from the definitions in ConfigDef.

```bash
# add to classpath

export CLASSPATH=my_connector.jar
java -jar build/libs/kafka-connect-helm-charts-0.3.1-3.3.0-all.jar

```