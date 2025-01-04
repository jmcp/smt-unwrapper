
# Unwrapper Transform for Kafka Connect

A Kafka Connect Simple Message Transform for unwrapping a hierarchical message data structure (the value part of the message).
The only configurable option is the log level, which defaults to *INFO*.

## License

This work is covered by the Hippocratic Licence v3, as documented in [LICENSE.md](LICENSE.md).


## Installation

Building the `JAR` file requires both Maven and Java to be installed. The `JAR` file can be built using Maven: `mvn package`

The project has been tested and built using Java 17 and Maven 3.9.2.

## Usage

Add the `JAR` file on the instance running Kafka Connect into a directory included in the `plugin.path` within the connectors config.

After restarting the Kafka Connect service the connector can then be configured to use the transform. An example is provided below.

```
"transforms": "Unwrapper",
"transforms.Unwrapper.type": "com.jmcpdotcom.transform.Unwrapper$Value",
```
