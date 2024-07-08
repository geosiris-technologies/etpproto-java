# ETPProto Java

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-orange.svg)](https://sonarcloud.io/summary/new_code?id=geosiris-technologies_etpproto-java)

## Introduction

[ETP](https://www.energistics.org/energistics-transfer-protocol/) standard implementation (client side)

## Features

A simple client to communicate with ETP standard (v2)

## Requirements

- Java 11
- Maven

## Version History

- 1.0.0: 
    - Initial Release

## License

This project is licensed under the Apache 2.0 License - see the `LICENSE` file for details

## Support

Please enter an issue in the repo for any questions or problems.

## Example : 

You can try the client locally with command line.

First, compile using *Example* profile :
```console
mvn clean package -P Example
```

Then launch the client : 
```console
java -jar ./target/etpproto-java-[PROJECT_VERSION]-jar-with-dependencies.jar ws://[YOUR_DOMAIN]:[YOUR_PORT]/[SUB_PATH]/ [LOGIN] [PASSWORD]
```


# TODO: 
Add the possibility to add a header value during websocket establishement : For OSDU it necessary to add in the header the "data-partition-id"