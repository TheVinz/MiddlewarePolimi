# Akka project

This project implements a BigData (batch and stream) processing engine using Akka actors. The engine accepts as input an acyclic graph of operators each accepting as input a key-value pair, where both key and value are Strings. The types of operators are:
* Map
* FlatMap
* Filter
* Aggregate
* Split
* Merge

## Usage

An example of application is described into the Test.java file.
For creating the graph a StreamBuilder class is given in the package common.graph.
A REST API is implemented on the port 8080 in order to communicate with the engine:
* **get /status**: returns a json representation of the current state of the engine
* **get /status/no_output**: returns a json representation of the current state of the engine, without showing the pairs collected by the sink
* **put /pair/{key}/{val}**: puts the pair (key, val) into the source queue
* **post /pairs**: puts the pairs contained into the given json file into the source queue. The json file must contain an array of objects, each with two parameters: 'key' and 'value'
* **post /stop**: stops the engine
For setting parameters like Actor's configurations and REST server's ip and port edit files present into the conf directory.
