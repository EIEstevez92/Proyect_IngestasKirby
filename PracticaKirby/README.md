# KIRBY

Ingestion & Masterization API and implementation with the following features:
 - Developed in Scala language
 - Distributed in-memory processing with Apache Spark
 - Modular design for inputs, transformations and outputs 
 - API easily extensible by developers
 - Read files from several formats
 - Apply multiple transformations to the original input data
 - Ensure you write an specific schema
 - Save files in many formats
 - One-Config to rule an entire pipeline
 - Built-in masterizations (eg: tokenization)

  ![Kirby](/api/doc/kirby.png?raw=true "Kirby Ingestion & Masterization")

### Set up
Check the [documentation](https://datiobd.atlassian.net/wiki/spaces/ingestas/pages/86548117/Manual+de+usuario+Kirby)

### Build project

For building kirby-ingestion.jar artifact:
```shell
mvn clean install -DskipTests 
```

## Running tests

For running application tests:
```shell
mvn clean test
```
For checking coverage:
```shell
mvn scoverage:report
```