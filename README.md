# cc-assignment1

## Dependencies
* [java SE dev kit](https://www.oracle.com/java/technologies/downloads/)
* [maven](https://maven.apache.org/download.cgi)
* [spark](https://spark.apache.org/downloads.html)

## Compiling and running
```console
# compile project
mvn clean package

# ensure script is executable
chmod +x run.sh

# run program
./run.sh
```

## todo
* run on cluster
* sanitise input (words) :white_check_mark: 
* sanitise input (letters - same as words but split into letters)
* count tokens using MapReduce
* sort tokens by frequency
* split list of tokens into categories (rare, popular, or common - discard the remaining tokens)
* sort each category by alphabetical order
* write into a csv file
* correctly package project
