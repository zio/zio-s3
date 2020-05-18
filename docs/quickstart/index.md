---
id: quickstart_index
title: "Quick Start"
---

Setup
-----

```
//support scala 2.12 / 2.13

libraryDependencies += "dev.zio" %% "zio-s3" % "<version>"
```


How to use it ?
---------------

```scala
import zio.s3._

  //list all files  
  listBuckets.provideLayer(
     live("us-east-1", S3Credentials("accessKeyId", "secretAccessKey"))
  )
```











