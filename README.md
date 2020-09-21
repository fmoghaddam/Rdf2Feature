# Rdf2Feature
Rdf2Feature is a Java framework for transforming RDF graphs to a 2D feature matrix which can be used in almost all Machine Learning algorithms. This framework is Cross-Platform, Open Source , Extensible and Easy to Use (Java version 1.8 or higher required).

# WorkFlow
The framwork is able to extracted all the possibie features from the given RDF data. It travers deeply the graph to reach a literal node and considers the full path from staring point to the literal as feature path. Following figure shows the system architecture overview. 


![Work-Flow](https://github.com/fmoghaddam/Rdf2Feature/blob/master/images/workflow.png "Workflow")

# Running 
**Inside Eclipse**

This project is based on [Gradle](https://gradle.org/). So it could be easily imported to Eclipse. For importing it the Eclipse should contain [Buildship Plugin](https://projects.eclipse.org/projects/tools.buildship).  After installing [Buildship Plugin](https://projects.eclipse.org/projects/tools.buildship), you can easily import the project into the Eclipse as a Gradle project.

The you need to run `./gradlew install` to install the framework. The last spet is modifing `config.properties` in `build/install/Rdf2Feature/bin` to a desire setting and finally running `build/install/Rdf2Feature/bin/Rdf2Feature.bat` in windows or `build/install/Rdf2Feature/bin/Rdf2Feature.sh` in unix machines.

**Running in Terminal**

For running the project from terminal:
```
$ git clone https://github.com/fmoghaddam/Rdf2Feature.git
$ cd Rdf2Feature
$ ./gradew install
$ cd build/install/Rdf2Feature/bin
$ ./Rdf2Feature.sh
```

**List of possibe configurations**

You need to modify the `config.properties` file before running the framwork. The list of possible configuration options are:
```
INPUT_DATA=/inputDataDirectory
OUTPUT_FILE=output.tsv

NUMNER_OF_THREADS=4
WALK_DEPTH=5
WALK_DEPTH_UP=1

SAMPLING_BY_PERCENTAGE=false
SAMPLING_PERCENTAGE=0.01
SAMPLING_NUNBER=100

#BOTH, OUT_GOING, IN_COMING
SERACH_DIRECTION=BOTH

SEED_GENERATION_SPARQL=SELECT ?person WHERE { ?person a dbo:Person.}
SEED_GENERATION_MAIN_VARIABLE=?person
```
