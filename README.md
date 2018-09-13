## Azakan Plugin(JobType)

这里提供两个我司在用HiveJobType和SparkJobType，实际上Azaban内建就有这两个JobType，只是我们觉得不太适合我们的应用场景。
我们希望简单，而且能够监控到Job正确的执行的状态，因此，我们重新定制Azkaban的这两个JobType。

### HiveJobType
基于Hive Beeline API实现的，非常简单。完全没有你想要权限什么的。

### SparkJobType
基于Spark-Launcher API实现的，同上。

如果你更关注任务的执行情况和状态、甚至是一些简单的Job日志的话。我觉得这两个JobType可能会你一个很好的选择。