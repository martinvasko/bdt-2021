# PySpark word count example

This is a pretty straightforward adaptation of the Java example code used to count occurrences of words in a text file using Apache Spark.

## Dependencies

You need PySpark. You can either set up a virtualenv or install it system wide; the former is usually recommended.

### virtualenv

Run the following *in this folder*:

```
virtualenv -p python3 .
source bin/activate
pip install -r requirements.txt
```

This sets up a virtualenv, activates it and installs pyspark (and any other dependencies listed in requirements.txt).

**You must use `source bin/activate` whenever you want to use this virtualenv, otherwise pyspark will not be available.**

### Plain/system wide

```
pip install pyspark
```


## Example usage

```
source bin/activate
python wordcount.py ../data/input/spark/500-loremipsum.txt output
cat output/part-00000
```
