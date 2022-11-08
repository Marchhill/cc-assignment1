# cc-assignment1

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

We then have to install the following python libraries using pip:
* Matplotlib
* Numpy
* Pandas

## Compiling and running
```console
# execute run-and-plot.py
nohup python3 -u run-and-plot.py > progress.out 2> progress.err &

# follow progress
tail -f progress.out

```
