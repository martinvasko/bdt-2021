import sys
from operator import add

from pyspark.sql import SparkSession

# Adapted (heavily rewritten) wordcount example
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <input file> <output file>", file=sys.stderr)
        sys.exit(-1)

    # Create SparkSession on local node with app name "Word Counter"
    spark = SparkSession.builder.master('local').appName('Word Counter').getOrCreate()

    # Load a text file into RDD
    lines = spark.sparkContext.textFile(sys.argv[1])

    # Smol lambda that makes a word into a tuple (with the second element "1")
    tuplify = lambda word: (word, 1)

    # Map: lines -> words (via split) -> tuplify -> flatten
    tuplified_words = lines.flatMap(lambda line: map(tuplify, line.split()))

    # Run the "add" operation over everything
    reduced = tuplified_words.reduceByKey(add)

    # Sort & output
    #for (word, total) in sorted(reduced.collect(), key=lambda x: x[1]):
    #    print(f'{word}: {total}')

    # ...or just save as text
    # Note that this doesn't actually "just" save a text file, but a folder
    reduced.sortBy(lambda x: x[1]).saveAsTextFile(sys.argv[2])

    # Bye, Sparkie!
    spark.stop()
