# Rheem Platform "Apache Spark"

`rheem-spark` is the repository, here you can found the implementation of the operators which allow the execution of 
platform "Apache Spark"

## Enviroment definition

With scala 2.11
```bash
SCALA_VERSION=scala-11
```
With scala 2.12
```bash
SCALA_VERSION=scala-12
```

### How to compile
```bash
mvn clean compile -P scala,${SCALA_VERSION}
```

### How to create the package

```bash
mvn clean package -P scala,${SCALA_VERSION}
```

### How to deploy

```bash
mvn clean deploy -P central,scala,${SCALA_VERSION}
```
