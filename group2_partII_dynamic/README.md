# cc-assignment2

## Dependencies
* [java SE dev kit](https://www.oracle.com/java/technologies/downloads/)
* [maven](https://maven.apache.org/download.cgi)
* [spark](https://spark.apache.org/downloads.html)

## Python
Python is required to run the scripts for the experiments. To install python on caelum, run

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh 
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
```

This installs Miniconda in a local directory. `.bashrc` should also be automatically modified
to add the miniconda bin to the path.

Run
```bash
which python3
which pip3
```
and ensure it points to the newly install miniconda installation.

## Compiling and running

The following commands should be run from the directory on caelum home directory that 'dynamic_configuration_parameter_scheduler.py' has been extracted to.
```console
# execute script
nohup python3 -u dynamic_configuration_parameter_scheduler.py [filename] > progress.out 2> progress.err &

# follow progress
tail -f progress.out

```

The script assumes that the filename is relative to the directory /test-data/

## Output format
The program will output to experiments/[current timestamp]/dynamic.csv

The first seven iterations will include the parameter configurations for all six values, as well as the execution time.
The rows representing the results of the subsequent thirteen iterations will only include the selected parameters, leaving the rest blank as they will be configured to their default values.