# Reservoir

[![Build Status](https://travis-ci.com/ylgrgyq/reservoir.svg?branch=master)](https://travis-ci.com/ylgrgyq/reservoir)
![Maven Project](https://img.shields.io/maven-central/v/com.github.ylgrgyq/reservoir?color=green)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.github.ylgrgyq%3Areservoir&metric=coverage)](https://sonarcloud.io/dashboard?id=com.github.ylgrgyq%3Areservoir)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Reservoir is a collection of queue-related classes which is used to store/retrieve elements to/from external storage  like file, redis etc. Currently, we only provided the local file based storage, but leave convient interfaces to adapt to other kind of storages.

## Features

* Data are arbitrary byte arrays or any serializable Java object.
* Data is persisted on local file system when write complete.
* With write-ahead-log, underlying file can survive from system crash. Any unfinished write on crashing will be aborted after recovery.
* Good performance. You can benchmark on your own machine with the provided benchmark command line tool.
* Highly extensible to adpat to other storage system in addition to the local file system. Actually we already implement a storage on Redis. We will release it when it is ready.
* Provide a convient tool which encapsulates common pattern to process elements from a queue.

## Usage examples

### Basic usage

Create a `ObjectQueue` instance.

```java
String fileStoragePath = // ..... like /tmp/reservoir
ObjectQueueStorage<byte[]> storage = FileStorageBuilder.newBuilder(tempDir).build();
ObjectQueue<byte[]> queue = ObjectQueueBuilder.newBuilder(storage).buildQueue();
```

Add some data to the end of the queue.`ObjectQueue` with `FileStorage` accepts a `byte[]` of arbitrary length.

```java
CompletableFuture<Void> future = queue.produce("Hello".getBytes(StandardCharsets.UTF_8));
```

When the returned future is completed, the added data has been saved on local file safely.

Retrieve data at the head of the queue.

```java
byte[] data = queue.fetch();
```

After fetch the data from the queue, we should commit this data and remove this data from the queue.

```java
queue.commit();
```

While `ObjectQueue` works with `byte[]`, it also works with arbitrary Java objects with a similar API. `ObjectQueue` requires a  [Codec](https://github.com/square/tape#converter)  to encode and decode objects.

```java
// Let us assume that there's a string codec which can encode/decode String objects
ObjectQueue<String> queue = ObjectQueueBuilder.newBuilder(fileStorage, stringCodec).buildQueue();

// produce a String object into the queue, not byte[]
queue.produce("Hello");
String data = queue.fetch();
queue.commit();
```

When you are done with `ObjectQueue`, you need to close it.

```java
queue.close();
```

### Codec

A `Codec` encodes or decodes objects of a type to another type which adapt to the storage provided to `ObjectQueue`.
For `FileStorage`, it can only accept `byte[]` as the serialized object type. So for the `Codec` used along with
`FileStorage`, it encodes objects to bytes and decodes objects from bytes.

```java
class StringCodec implements Codec<String, byte[]> {
    @Override
    public byte[] serialize(String obj) throws SerializationException {
        return obj.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) throws DeserializationException {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
```

### Automatic queue consumer

It is a common pattern to consume an object from a queue then do some stuff with it. If everything goes well, when we are done with this object, we commit it from the queue and continue to fetch, process the next object on the queue. So we provide `AutomaticObjectQueueConsumer` as a tool to encapsulate these things.

At first we need to define a task which implement `Verifiable` for `AutomaticObjectQueueConsumer` to process. With `Verifiable`, when a task consumed from `ObjectQueue`, `AutomaticObjectQueueConsumer` can check if this task is still valid. If not, it can skip this task directly.

```java
class Dinner implements Verifiable {
    @Override
    public boolean isValid() {
        return !spoiled();
    }

    public void enjoy() {
        // having dinner
    }
}
```

Then we need to define a handler to process the task defined above. 

```java
class Bob implements ConsumeObjectHandler<HavingDinner> {
    @Override
    public void onHandleObject(Dinner myDinner) throws Exception {
        myDinner.enjoy();
    }

    @Override
    public HandleFailedStrategy onHandleObjectFailed(Dinner myDinner, Throwable throwable) {
        log.warn("Bob failed to enjoy his dinner: {}", myDinner, throwable);

        // Bob can ignore this dinner or return RETRY to give this dinner another chance
        // or return SHUTDOWN to smash the table furiously and don't have any dinner anymore
        return HandleFailedStrategy.IGNORE;
    }
}
```

Finally, we need to create `ObjectQueue` and pass it with `ConsumeObjectHandler` to `AutomaticObjectQueueConsumer`.

```java
ObjectQueue<HavingDinner> queue = // .... create queue like before
AutomaticObjectQueueConsumer consuemr = new AutomaticObjectQueueConsumer(queue, new Bob());
```

Then, when any `Dinner` produced to the queue, `Bob` will consume it. You can also provide `ConsumeObjectListener` to `AutomaticObjectQueueConsumer` to monitor the process of `Bob` having his `Dinner`.

When you are done with `AutomaticObjectQueueConsumer`, you need to close it. It will close the `ObjectQueue` within too.

```java
consumer.close();
```

# Benchmark

We provide a easy to use (I think) benchmark tool to test the performance of Reservoir in your local environment. Currently, the benchmark tool only covers the storage performance, but it's the major and the most time consuming part of Reservoir. 

Firstly, please navigate to the path: `./reservoir-benchmark/`. 

For benchmark on write operation, please use `./bin/benchmark storage write` command with options you like after checking the help command:

```
./bin/benchmark storage write -h
Usage:

Test the writing performance of the storage.

benchmark storage write [-h] [--sync-write-wal] [-c=<coolDownSecs>]
                        [-n=<numberOfBatches>] [-p=<numberOfDataPerBatch>]
                        [-s=<dataSize>] [-t=<testingTimes>] [-T=<storageType>]
                        [-w=<warmUpTimes>]

Description:

All the tests in this command is only used to test the writing performance of
the storage. During the test, no read operations will be issued. With the
options of this command, you can test the storage in different working
conditions.

Options:
  -h, --help             Show this help message and exit.
  -s, --data-size=<dataSize>
                         Size in bytes of each data to write.
                           Default: 100
  -p, --number-of-data-per-batch=<numberOfDataPerBatch>
                         Number of data per batch to write.
                           Default: 10
  -n, --number-of-batches=<numberOfBatches>
                         Number of batches of data to write for each tests.
                           Default: 10000
  -w, --warm-up-times=<warmUpTimes>
                         Warm-up times before the start of the official tests.
                           Default: 5
  -t, --testing-times=<testingTimes>
                         Official testing times after warm-up period.
                           Default: 3
  -c, --cool-down-interval-secs=<coolDownSecs>
                         Cool down interval in seconds between each tests.
                           Default: 5
      --sync-write-wal   Flush underlying WAL log in storage synchronously
                           after every write.
  -T, --storage-type=<storageType>
                         The underlying storage type used by this test. 
                           Default: FileStorage
```

For benchmark on read operation, please use `./bin/benchmark storage read` command with options you like after checking the help command:                             

```
./bin/benchmark storage read -h 
Usage:

Test the reading performance of the storage.

benchmark storage read [-hS] [--sync-write-wal] [-c=<coolDownSecs>]
                       [-n=<numOfDataToRead>] [-p=<readBatchSize>]
                       [-s=<dataSize>] [-t=<testingTimes>] [-T=<storageType>]
                       [-w=<warmUpTimes>]

Description:

All the tests in this command is only used to test the reading performance for
the storage. During the test setup period, data for the read test will be
written to storage. After that, no more write operations will be issued. With
the options of this command, you can test the storage in different working
conditions.

Options:
  -h, --help             Show this help message and exit.
  -s, --data-size=<dataSize>
                         Size in bytes of each data.
                           Default: 100
  -p, --number-of-data-per-read=<readBatchSize>
                         Number of data to retrieve in one read.
                           Default: 10
  -n, --total-number-of-data-to-read=<numOfDataToRead>
                         Total number of data to read.
                           Default: 10000
  -S, --random-read      Read data in random order, otherwise read will be in
                           sequential order.
  -w, --warm-up-times=<warmUpTimes>
                         Warm-up times before the start of the official tests.
                           Default: 5
  -t, --testing-times=<testingTimes>
                         Official testing times after warm-up period.
                           Default: 3
  -c, --cool-down-interval-secs=<coolDownSecs>
                         Cool down interval in seconds between each tests.
                           Default: 5
      --sync-write-wal   Flush underlying WAL log in storage synchronously
                           after every commit.
  -T, --storage-type=<storageType>
                         The underlying storage type used by this test. 
                           Default: FileStorage
```

# License

Copyright 2019 Rui Guo. Released under the [MIT License](https://github.com/ylgrgyq/reservoir/blob/master/LICENSE.md).
