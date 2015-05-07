# Spark Hands on

## Summary

1. Prerequisite
2. Load Datasets
3. Small exercices on datasets
4. Machine Learning


## Prerequisite

### General

* JDK 7 or higher
* SBT 13 or higher
* Your favorite scala IDE (For instance IntelliJ with Scala/SBT plugin)

### Project

* Clone this repository
```
git clone https://github.com/vincentdoba/spark-hands-on.git
```
* Go at root of the cloned repository
* Run word count on this README file 
```
sbt "run-main psug.hands.on.prerequisite.WordCount README.md"  
```

## Exercises

### Exercise 1

#### Description

Count the number of departments in France, given a file containing the list of department

#### Notions

* Create a SparkContext
* Load a simple text file
* First action : count

### TODO Exercise 2

#### Description

?

#### Notions

* Discover actions
* Discover transformation

### Exercise 3

#### Description

How many inhabitants has France ?

#### Notions

* Create Spark SQL context
* Load a JSON file
* First actions and transformations on DataFrames

### Exercise 4

#### Description

What are the ten densest departments in France ?

#### Notions

* DataFrames actions and transformations
* Join

### Exercise 5

#### Description

What are the characteristics of the densest city in France ? Preparation of features for Machine Learning Algorithms

#### Notions

* Some transformations on DataFrames

### Exercise 6

#### Description

Normalize data for a Machine Learning algorithm

#### Notions

* Aggregate function
* Save to a JSON file

### Exercise 7

#### Description

Guess which cities in France have more than 5000 inhabitants given some features such as Density, percentage of executives
in total population, percentage of workers, percentage of employees and percentage of farmers

#### Notions

* New Spark Machine Learning Library

### TODO : Exercise 8

#### Description

Run spark script on a spark cluster

#### Notions

* Spark deployment and usage