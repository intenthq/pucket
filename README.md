[![Build Status](https://travis-ci.org/intenthq/pucket.svg?branch=master)](https://travis-ci.org/intenthq/pucket)

> Parquet + Bucket = Pucket.

Pucket is Scala library which provides a simple partitioning system for Parquet.



## Pucket Design

Pucket has been designed to be a simple wrapper around Parquet, following the design principals below

* Simple programming interface for writing custom partitioning schemes
* Functionally orientated - controlled side effects and no mutable state
* Limited set of core functionality
* Limited abstraction from underlying frameworks - don't attempt to hide necessary complexity

With Pucket we aim to provide the following high level features, broken down into single responsibility modules:

* Simple set of functionality for bucketing data and maintaining schemas
* Filesystem level partitioning of Parquet files
* Incremental writing of Parquet files with checkpoints
* Integration with MapReduce and Spark

### Pucket High Level Concepts and Usage Considerations

Pucket's implementation is centered around a few key concepts described below. These may be a one time implementation in the core functionality or is partially implemented and requires specific implementation per data format. Current Pucket supports Avro and Thrift, but can be easily extended to support Protocol Buffers.

#### Pucket

(_Implementation per format_)

This is a partially implemented trait which contains information on the the data in a certain directory (or bucket) on the filesystem. It has a few simple operations for creating a new instance:

**Create** - create a new Pucket by writing a descriptor to the filesystem, if a pucket already exists at the location an error will be returned

**Find** (apply function) - return a Pucket which is known to be at a certain location on the filesystem, if it does not exist an error will be returned

**Find or create** - return an existing Pucket or create a new one, will return an error if the existing Pucket's descriptor does not match the one provided

Once an instance is create the following operations can be performed on it:

**Reader** - obtain a reader for the Pucket

**Writer** - obtain a simple writer for the Pucket

**Absorb** - move another Pucket's data into this one (provided they are the same structure)

**List files** - [ronseal](https://wikipedia.org/en/Does_exactly_what_it_says_on_the_tin)

It also holds configuration for the default Parquet block size: i.e. the amount of data the underlying Parquet writer will hold in memory before flushing it to disk.

#### Descriptor

The Pucket descriptor is a class which describes the structure of the partitions and the data within the Pucket. It is written to the filesystem as JSON on creation and read when the Pucket is located. The descriptor on disk contains the following information:

* Schema format
* Compression used
* Partitioner (optional)

#### Partitioner

The partitioner is a trait which describes the partitioning scheme, to be implemented by the user according to the requirements for partitioning their data. The class name of the partitioner is stored in the descriptor, so the implementation can change. While it is not recommended to change the implementation on an existing Pucket, your data will still be accessible for reading in the old scheme.

#### Writer

There are a few implementations of writer for Pucket, each performing a different type of writing functionality. Each type is described below and code examples can be seen in the [TL;DR](#tldr) section.

**Simple Writer** (_Implementation per format_) - a functional wrapper around the standard implementations of Parquet writers

**Incremental Writer** - a wrapper around the simple writer which rolls a file when a configured number of objects have been written. Keeps a checkpoint of the point at which a file was last finalised. **It is important to tune the roll limit based on expected size of each data object**, this should be a balance of number of objects you are prepared to lose per checkpoint vs the number of small files on the filesystem, given Hadoop's default block size.

**Partitioned Writer** - a wrapper around the simple writer which uses the partitioner implementation to write data out to sub directories of the Pucket. The writer instances are kept in a cache with a configurable size, when the cache is full the least recently used writer will be removed and closed. **It is important to make sure you balance the writer cache size with your memory constraints and number of partitions you expect to be writing to concurrently**, as opening and closing writers is an expensive operation. You should also be aware of the Parquet block size configuration in the Pucket, by default each writer will hold 50mb in memory before flushing to disk.

**Incremental Partitioned Writer** - a wrapper around the incremental writer which provides partitioning. The same tuning constraints apply to this as with the incremental and partitioned writers.

#### Reader

On setting out to implement Pucket there were no plans to implement a reader, however the standard Parquet reader cannot read files in subdirectories and assumes that all files in a given directory are Parquet format. Therefore we had to clone the functionality in the main Parquet reader and change it to allow reading of files in subdirectories. _Note that the Parquet input format for MapReduce can cope with parquet files in subdirectories so does not need to use this reader_.

TL;DR. Show Me Some Code

* Pucket
* PucketDescriptor
* Partitioner
* Reader
* Writer

The Scalaz implementation of `Either`, known as disjunction[^4] (`\/`), is used heavily within the Pucket code. This is a proper Monad which allows it to be used in a flat map or for comprehension. This enables a happy path and sad path to be accounted for when performing any side-effecting operation. Every operation requiring interaction with the Hadoop filesystem will return a disjunction.

In the examples below disjunction is used with the implicit either class in Scalaz syntax package, which allows `.left` or `.right` operations to lift objects into the appropriate side of the disjunction. To use this the following imports must be included in implementing classes:

{% highlight scala %}
import scalaz.\/
import scalaz.syntax.either._
{% endhighlight %}

### Creating a Pucket

The following examples use the imports listed below:

{% highlight scala %}
import com.intenthq.pucket.thrift.ThriftPucket
import com.intenthq.pucket.thrift.ThriftPucketDescriptor
import com.intenthq.pucket.avro.AvroPucket
import com.intenthq.pucket.avro.AvroPucketDescriptor
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.hadoop.fs.{FileSystem, Path}
import scalaz.\/
import scalaz.syntax.either._
{% endhighlight %}

You should also make sure you have created the following classes:

{% highlight scala %}
import your.thrift.ThriftData
import your.pucket.ThriftPartitioner
import your.avro.AvroData
import your.pucket.AvroPartitioner
{% endhighlight %}

The following values have also been created:

{% highlight scala %}
val fs = FileSystem.get()
val path = new Path("/path/to/Pucket")
{% endhighlight %}

Create or find a Thrift Pucket:

{% highlight scala %}

val thriftDescriptor: ThriftPucketDescriptor[ThriftData] =
  ThriftPucketDescsriptor[ThriftData](classOf[ThriftData],
                                      CompressionCodecName.SNAPPY,
                                      Some(ThriftPartitioner))

// Create a new Pucket on /path/to/Pucket, writing the descriptor in place
// Operation will fail if a Pucket already exists on that path
val newThriftPucket: Throwable \/ Pucket[ThriftData] =
  ThriftPucket.create[ThriftData](path, fs, thriftDescriptor)

// Find an existing Pucket at a certain path
// Operation will fail if no Pucket exists on that path or the schema does not match the one provided
val existingThriftPucket: Throwable \/ Pucket[ThriftData] =
  ThriftPucket[ThriftData](path, fs, classOf[ThriftData])

// Find an existing or create a new Pucket on a certain path
// Operation will fail if the Pucket exists and the Pucket descriptor on the filesystem matches the one provided
val maybeExistingThriftPucket: Throwable \/ Pucket[ThriftData] =
  ThriftPucket.findOrCreate[ThriftData](path, fs, thriftDescriptor)

{% endhighlight %}

Create or find an Avro Pucket:

{% highlight scala %}

val avroDescriptor: AvroPucketDescriptor[AvroData] =
  AvroPucketDescriptor[AvroData](AvroData.getClassSchema,
                                 CompressionCodecName.SNAPPY,
                                 Some(AvroPartitioner))

val newAvroPucket: Throwable \/ Pucket[AvroData] =
  AvroPucket.create[AvroData](path, fs, avroDescriptor)

val existingAvroPucket: Throwable \/ Pucket[AvroData]
  AvroPucket[AvroData](path, fs, AvroData.getClassSchema)

val maybeExistingAvroPucket: Throwable \/ Pucket[AvroData] =
  AvroPucket.findOrCreate[AvroData](path, fs, avroDescriptor)


{% endhighlight %}

### Writing to a Pucket


{% highlight scala %}
// Write function which fails fast on error
def write[Ex](data: Seq[T],
              writer:  Ex \/ Writer[T, Ex]): Ex \/ Writer[T, Ex] =
  data.foldLeft(writer)( (w, i) =>
    w.fold(ex => return ex.left, _.write(i))
  )
{% endhighlight %}

**Plain Writer**

{% highlight scala %}
def writeMeSomeData[T](data: Seq[T],
                       Pucket: Throwable \/ Pucket[T]): Throwable \/ Unit =
  for {
    p <- pucket
    writer <- p.writer
    finishedWriter <- write[Throwable](data, writer)
    _ <- finishedWriter.close
  } yield ()
{% endhighlight %}

**Incremental Writer**

{% highlight scala %}
def writeMeSomeDataIncrementally[T](data: Seq[T],
                                    Pucket: Throwable \/ Pucket[T]): (Long, Throwable) \/ Unit =
  for {
    p <- pucket.leftMap((0, _))
    writer <- IncrementalWriter(p, 100) // 100 indicates the number of writes before the file is rolled
    finishedWriter <- write[(Long, Throwable)](data, writer)
    _ <- finishedWriter.close
  } yield ()
{% endhighlight %}

**Partitioned Writer**

{% highlight scala %}
def writeMeSomePartitionedData[T](data: Seq[T],
                                  Pucket: Throwable \/ Pucket[T]): Throwable \/ Unit =
  for {
    p <- pucket
    writer <- PartitionedWriter(p).right
    finishedWriter <- write[Throwable](data, writer)
    _ <- finishedWriter.close
  } yield ()
{% endhighlight %}

**Incremental Partitioned Writer**

{% highlight scala %}
def writeMeSomePartitionedDataIncrementally[T](data: Seq[T],
                                              Pucket: Throwable \/ Pucket[T]): (Long, Throwable) \/ Unit =
  for {
    p <- pucket.leftMap((0, _))
    writer <- IncrementalPartitionedWriter(p, 100).right
    finishedWriter <- write[Throwable](data, writer)
    _ <- finishedWriter.close
  } yield ()
{% endhighlight %}

### Reading From a Pucket

The reader behaves in a similar way to the writer in that each read returns a new instance of the reader with updated state, however as well as a new reader instance, an option of the data item is returned. The example below is an implementation which will read a certain number of records into a scala `Seq` or fail with a `Throwable`. If there is an error encountered in the read process then the code will fail fast and return the throwable in the left side of the disjunction. If the output from the pucket is exhausted then it will close the reader and return the result in the right side of the disjunction.

{% highlight scala %}
def readData[T](count: Int, pucket: Pucket[T]): Throwable \/ Seq[T] =
  pucket.reader.flatMap(reader =>
    0.to(count).foldLeft((Seq[T](), reader).right[Throwable])( (acc, _) =>
      acc.fold(ex => return ex.left,
       dataAndReader => dataAndReader._2.read.fold(ex => return ex.left,
         optionalDataAndReader => (
           optionalDataAndReader._1.fold(
             // if the output is exhausted, then close the reader and return the state
             return optionalDataAndReader._2.close.map(_ => dataAndReader._1)  
              //if the data is present in the output then append the data to the state and include the updated writer state
            )(dataAndReader._1 ++ Seq(_)), optionalDataAndReader._2).right[Throwable]
        )
      )
    )
  ).flatMap(dataAndReader => dataAndReader._2.close.map(_ => dataAndReader._1))
{% endhighlight %}

### More examples

For more detailed examples such as working with MapReduce and Spark please see the [Pucket GitHub repository](https://github.com/intenthq/pucket).
