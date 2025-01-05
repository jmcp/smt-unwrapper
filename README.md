
# Unwrapper Transform for Kafka Connect

A Kafka Connect Simple Message Transform for unwrapping a hierarchical message data structure (the value part of the message).
The only configurable option is the log level, which defaults to *INFO*.

## License

This work is covered by the Hippocratic Licence v3, as documented in [LICENSE.md](LICENSE.md).

## Installation

Building this project requires Java 17 to be installed, and the use of Maven. If you have Maven v3.9.2 or better
available to you locally, then run `mvn package` to compile and generate the package. Alternatively, you can use the
Maven wrapper included with this repo.

In Unix-like environments (Linux, Mac OS etc) run

`./mvnw package`

On Windows you can run

`.\mvnw.cmd package`

To find out more about the Maven wrapper, please visit https://www.baeldung.com/maven-wrapper

## Usage

Add the `JAR` file on the instance running Kafka Connect into a directory included in the `plugin.path` within the connectors config.

After restarting the Kafka Connect service the connector can then be configured to use the transform. An example is provided below.

```
"transforms": "Unwrapper",
"transforms.Unwrapper.type": "com.jmcpdotcom.transform.Unwrapper$Value",
```
