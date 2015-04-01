# dslink-java-haystack

[![Build Status](https://drone.io/github.com/IOT-DSA/dslink-java-haystack/status.png)](https://drone.io/github.com/IOT-DSA/dslink-java-haystack/latest)

A DSLink that works with Haystack. More information about Haystack can be
located at <http://project-haystack.org/>

## Distributions

Distributions can be ran independent of Gradle and anywhere Java is installed.
Prebuilt distributions can be found [here](https://drone.io/github.com/IOT-DSA/dslink-java-haystack/files).

### Creating a distribution

Run `./gradlew distZip` from the command line. Distributions will be located
in `build/distributions`.

### Running a distribution

Run `./bin/dslink-java-haystack -b http://localhost:8080/conn` from the command
line. The link will then be running.

## Test running

A local test run requires a broker to be actively running.

Running: <br />
`./gradlew run -Dexec.args="--broker http://localhost:8080/conn"`
