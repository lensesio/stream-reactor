# Lenses Connectors for Apache Kafka
## Github Actions Workflow Guide

We have 2 Github workflows, one for build (continuous integration) and one for release.

### Generating the Module Lists

The Github Actions workflow uses several "module" lists to inform it which project modules need which jobs running.

You can generate these module lists on your local copy by running the following sbt command:

```shell
sbt generateModulesList generateItModulesList generateFunModulesList generateDepCheckModulesList
```
These generate a series of files in the project:

```
target/scala-2.13/resource_managed/main/modules.txt
target/scala-2.13/resource_managed/main/it-modules.txt
target/scala-2.13/resource_managed/main/fun-modules.txt
target/scala-2.13/resource_managed/main/depcheck-modules.txt
```

This is the current contents of modules.txt:
```json
["query-language","common","sql-common","cloud-common","aws-s3","azure-documentdb","azure-datalake","cassandra","elastic6","elastic7","ftp","gcp-storage","http","influxdb","jms","mongodb","mqtt","redis"]
```

The aim of these files is to instruct the workflow of what the modules are.  If you wish to look at how these are built then look at the bottom of `build.sbt`.

* `modules.txt` lists all the modules of the Stream Reactor project
* `it-modules.txt` lists only those which have an integration test configuration defined
* `fun-modules.txt` lists only those which have a functional test configuration defined
* `depcheck-modules.txt` lists only those which produce a final artifact for the build (e.g. it excludes `kafka-connect-query-language` which does not need to be published independently)



### Reading the Modules Lists

Because the project is divided into multiple modules, the first step of the Github workflow asks SBT what the modules are, and reads them into variables for use throughout the workflows:

```yaml
jobs:
  initiate:
    timeout-minutes: 5
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.read-mods.outputs.matrix }}
      it_matrix: ${{ steps.read-mods.outputs.it-matrix }}
      fun_matrix: ${{ steps.read-mods.outputs.fun-matrix }}
      dep_check_matrix: ${{ steps.read-mods.outputs.dep-check-matrix }}
    steps:
      - name: Generate modules lists
        run: sbt generateModulesList generateItModulesList generateFunModulesList generateDepCheckModulesList
        env:
          JVM_OPTS: -Xmx3200m
      - name: Read modules lists
        id: read-mods
        run: |
          echo "matrix=$(cat ./target/scala-2.13/resource_managed/main/modules.txt)" >> $GITHUB_OUTPUT
          echo "it-matrix=$(cat ./target/scala-2.13/resource_managed/main/it-modules.txt)" >> $GITHUB_OUTPUT
          echo "fun-matrix=$(cat ./target/scala-2.13/resource_managed/main/fun-modules.txt)" >> $GITHUB_OUTPUT
          echo "dep-check-matrix=$(cat ./target/scala-2.13/resource_managed/main/depcheck-modules.txt)" >> $GITHUB_OUTPUT
```

### Running Jobs Based on Modules

The Github workflows use a Project Matrix that is designed to run a given job for each component in the matrix.

Here is a job from our yaml file that is running under the `matrix` matrix, which includes all modules of the SBT project.

```yaml
jobs:
  test:
    needs:
      - initiate
      - scalafmt
    timeout-minutes: 5
    runs-on: ubuntu-latest
    strategy:
      matrix:
        module: ${{fromJSON(needs.initiate.outputs.matrix)}}
    steps:
      - ...
```

To use the matrices prepared in the `initiate` job you must declare a dependency via the `needs` keyword, and then use the `strategy` to refer to the matrix.  We use `fromJson` to convert the string value of the matrix into an array that the github runners can iterate over.

Now everything under `steps` will be actioned for each module under the matrix.

