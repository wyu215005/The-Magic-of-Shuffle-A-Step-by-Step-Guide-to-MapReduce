# MapReduce 执行全流程：Shuffle 的魔法（经典必读）

> 本文详细剖析 Hadoop MapReduce 的核心执行流程，重点解析被称为"灵魂"的 Shuffle 阶段，涵盖 InputSplit、RecordReader、Partition、Sort、Spill、Merge、Combiner 以及 Reduce 的完整工作原理，做到图文并茂、有理有据。

---

## 目录

1. [MapReduce 总体架构](#1-mapreduce-总体架构)
2. [InputSplit & RecordReader：数据如何切分并进入 Map](#2-inputsplit--recordreader数据如何切分并进入-map)
3. [Map 阶段](#3-map-阶段)
4. [Shuffle 阶段（核心灵魂）](#4-shuffle-阶段核心灵魂)
   - 4.1 [环形缓冲区（Circular Buffer）](#41-环形缓冲区circular-buffer)
   - 4.2 [Partition（分区）](#42-partition分区)
   - 4.3 [Sort（排序）](#43-sort排序)
   - 4.4 [Spill（溢写）](#44-spill溢写)
   - 4.5 [Combiner（本地归约）](#45-combiner本地归约)
   - 4.6 [Merge（归并）](#46-merge归并)
5. [Reduce 阶段](#5-reduce-阶段)
6. [完整数据流总览](#6-完整数据流总览)
7. [关键参数调优](#7-关键参数调优)
8. [总结](#8-总结)

---

## 1. MapReduce 总体架构

MapReduce 是一种大规模数据处理的**编程模型**，由 Google 于 2004 年在论文 *MapReduce: Simplified Data Processing on Large Clusters* 中提出，Hadoop 实现了其开源版本。

其核心思想是**分而治之**：

- **Map**：将大规模输入数据分割为独立的小块，并行处理，生成中间 `<key, value>` 键值对。
- **Shuffle**：将 Map 的输出按照 key 进行分区、排序、传输，送达对应的 Reducer。
- **Reduce**：对相同 key 的所有 value 进行聚合，产生最终结果。

```
┌──────────────────────────────────────────────────────────────────┐
│                        MapReduce 作业流程                          │
│                                                                    │
│  HDFS Input                                                        │
│  ┌─────────┐   InputSplit    ┌──────────┐                          │
│  │  Block1 │ ─────────────► │  Map 1   │ ──┐                      │
│  │  Block2 │ ─────────────► │  Map 2   │ ──┤  Shuffle             │
│  │  Block3 │ ─────────────► │  Map 3   │ ──┤  (Partition+Sort     │
│  └─────────┘                └──────────┘   │   +Spill+Merge)      │
│                                            │                       │
│                             ┌──────────┐ ◄─┤                      │
│                             │ Reduce 1 │   │  ┌─────────┐         │
│                             │ Reduce 2 │ ──┼► │  HDFS   │         │
│                             │ Reduce 3 │   │  │ Output  │         │
│                             └──────────┘ ◄─┘  └─────────┘         │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. InputSplit & RecordReader：数据如何切分并进入 Map

### 2.1 InputSplit（输入切片）

**InputSplit** 是 MapReduce 对输入数据的**逻辑切分**单位，每一个 InputSplit 对应一个 Map Task。

> ⚠️ 注意：InputSplit 是逻辑概念，不是物理切割文件，它只记录数据的位置和长度信息。

**切片过程（以 `FileInputFormat` 为例）：**

1. 客户端在提交 Job 前，调用 `InputFormat.getSplits()` 计算切片。
2. 默认切片大小 = HDFS Block 大小（Hadoop 2.x 默认 128 MB）。
3. 计算逻辑：

```
splitSize = max(minSize, min(maxSize, blockSize))
```

| 参数 | 配置项 | 默认值 |
|------|--------|--------|
| minSize | `mapreduce.input.fileinputformat.split.minsize` | 1 |
| maxSize | `mapreduce.input.fileinputformat.split.maxsize` | Long.MAX_VALUE |
| blockSize | HDFS 块大小 | 128 MB |

**切片与 Block 的关系图：**

```
HDFS 文件（300 MB）
┌─────────────────────────────────────────────────────────┐
│   Block 1 (128 MB)  │  Block 2 (128 MB)  │ Block 3(44MB)│
└─────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
   InputSplit 1         InputSplit 2         InputSplit 3
   (0 ~ 128MB)         (128 ~ 256MB)        (256 ~ 300MB)
         │                    │                    │
         ▼                    ▼                    ▼
      Map Task 1          Map Task 2           Map Task 3
```

**数据本地化（Data Locality）：** JobTracker/ResourceManager 会尽量将 Map Task 调度到持有该 Block 副本的 DataNode 上执行，从而减少网络传输（移动计算，而非移动数据）。

### 2.2 RecordReader（记录读取器）

**RecordReader** 负责将 InputSplit 中的字节流解析为 `<key, value>` 记录，逐条喂给 Mapper。

以最常用的 `TextInputFormat` 为例：

| 格式 | key | value |
|------|-----|-------|
| `TextInputFormat` | 行在文件中的字节偏移量（LongWritable） | 该行文本内容（Text） |
| `KeyValueTextInputFormat` | 制表符前的字段 | 制表符后的字段 |
| `SequenceFileInputFormat` | 自定义类型 | 自定义类型 |

**RecordReader 工作流程：**

```
InputSplit（字节范围）
       │
       ▼
  RecordReader
  ┌──────────────────────────────────┐
  │  1. 打开对应 HDFS Block 的流      │
  │  2. 定位到 split 起始位置         │
  │  3. 跳过不完整的首行（行边界对齐） │
  │  4. 逐行读取，生成 <offset, line> │
  │  5. 到达 split 末尾时停止         │
  └──────────────────────────────────┘
       │
       ▼  每次调用 nextKeyValue()
  <key, value> ──► Mapper.map()
```

> **行边界对齐**：当 InputSplit 在某行中间截断时，RecordReader 会继续读取直到行尾（允许跨越 split 边界），确保每行数据完整处理。

---

## 3. Map 阶段

Mapper 接收 RecordReader 产生的 `<key, value>` 对，执行用户自定义的 `map()` 函数，将其转换为新的 `<key, value>` 中间结果并输出。

**Map 输出写入环形缓冲区（kvbuffer）：**

```java
// 用户实现示例：WordCount 的 Mapper
public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+");
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
            // 输出：<"hello", 1>, <"world", 1>, ...
        }
    }
}
```

Map 的输出不会直接写磁盘，而是先写入内存中的**环形缓冲区**，这是 Shuffle 的起点。

---

## 4. Shuffle 阶段（核心灵魂）

Shuffle 是 MapReduce 性能的关键所在，它完成了 Map 输出到 Reduce 输入的整个数据传输过程。

```
Map 输出
  │
  ▼
环形缓冲区（内存，默认 100 MB）
  │  达到阈值（默认 80%）
  ▼
Partition + Sort（内存中排序）
  │
  ▼
Spill（溢写到磁盘）── 可选：Combiner 本地预聚合
  │
  ▼
多个 Spill 文件 Merge（归并）── 可选：Combiner
  │
  ▼
Map 最终输出文件（已分区、已排序）
  │
  │  HTTP 传输（Shuffle 网络阶段）
  ▼
Reducer 拉取（Fetch）自己分区的数据
  │
  ▼
Reduce 端 Merge（归并多个 Map 的输出）
  │
  ▼
Reducer 输入（按 key 分组的迭代器）
```

### 4.1 环形缓冲区（Circular Buffer）

Map 输出的每条 `<key, value>` 记录不直接写磁盘，而是先写入一块内存区域——**环形缓冲区**（`kvbuffer`）。

**设计亮点：**
- 环形结构避免了内存碎片，实现了高效的连续写入。
- 缓冲区同时存储**键值数据**（顺时针写入）和**索引信息**（逆时针写入），两者从缓冲区两端相向写入。

```
环形缓冲区（100 MB，默认）
┌──────────────────────────────────────────────────────┐
│◄── 索引(kvmeta) │             空闲              │ 数据(kvbuffer) ──►│
│   partition     │                               │ key bytes        │
│   keystart      │   ← equator →                 │ value bytes      │
│   valstart      │                               │                  │
│   vallen        │                               │                  │
└──────────────────────────────────────────────────────┘
    逆时针写入 ◄                                    ► 顺时针写入
```

| 参数 | 配置项 | 默认值 |
|------|--------|--------|
| 缓冲区总大小 | `mapreduce.task.io.sort.mb` | 100 MB |
| 溢写触发阈值 | `mapreduce.map.sort.spill.percent` | 0.80（即 80%） |

### 4.2 Partition（分区）

Partition 决定每条 Map 输出记录**应该发送给哪个 Reducer**。

**默认分区器（`HashPartitioner`）：**

```java
public int getPartition(K key, V value, int numReduceTasks) {
    return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
}
```

- 通过对 key 的 hash 值取模，确保相同的 key 总是路由到同一个 Reducer。
- 分区号（partition id）与每条记录一同存储在索引区。

**分区数 = Reduce Task 数量：**

```
Map 输出
┌─────────────────────────────────────────────┐
│  <"apple",1>  │ partition 0 (Reducer 0 处理) │
│  <"banana",1> │ partition 1 (Reducer 1 处理) │
│  <"cherry",1> │ partition 2 (Reducer 2 处理) │
│  <"apple",1>  │ partition 0 (Reducer 0 处理) │
└─────────────────────────────────────────────┘
```

> **自定义分区**：当需要控制数据分布（例如避免数据倾斜）时，可继承 `Partitioner` 实现自定义逻辑。

### 4.3 Sort（排序）

当缓冲区使用量达到阈值（默认 80%）时，后台线程 `SpillThread` 被唤醒，对缓冲区中的记录进行**内存内排序**。

**排序对象：** 仅对索引区（kvmeta）进行排序，实际键值数据不移动，减少内存复制开销。

**排序规则：**
1. **首先按 partition 编号升序排列**（确保同一分区的数据连续）
2. **同一 partition 内按 key 升序排列**（使用 `RawComparator` 直接比较序列化字节）

```
排序前（索引区）：
[partition=1, key="banana"] [partition=0, key="cherry"] [partition=0, key="apple"]

排序后（索引区）：
[partition=0, key="apple"] [partition=0, key="cherry"] [partition=1, key="banana"]
```

**RawComparator 的优势：** 直接比较序列化后的字节，无需反序列化为 Java 对象，极大提升了排序性能。

### 4.4 Spill（溢写）

排序完成后，`SpillThread` 将排好序的数据**顺序写入磁盘**，称为溢写（Spill）。

**溢写过程：**

```
内存缓冲区（已排序）
         │
         ▼  溢写线程（后台，与 map() 并行）
┌─────────────────────────────────────────────────────────┐
│  Spill 文件（磁盘）                                       │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Partition 0: <apple,1> <cherry,1>               │   │
│  │  Partition 1: <banana,1>                         │   │
│  │  Partition 2: <date,1>                           │   │
│  └──────────────────────────────────────────────────┘   │
│  + 索引文件（记录每个 partition 的偏移量和长度）            │
└─────────────────────────────────────────────────────────┘
```

- 每次溢写生成一个 `spill{N}.out` 文件和对应的 `spill{N}.out.index` 索引文件。
- 溢写是**异步**进行的：溢写线程写磁盘的同时，Map 继续向另一半缓冲区写新数据。
- 若 Map 写入速度超过溢写速度导致缓冲区满，Map 会**阻塞等待**。

**磁盘溢写路径：** 由 `mapreduce.cluster.local.dir` 指定，通常为 TaskTracker 本地磁盘目录。

### 4.5 Combiner（本地归约）

Combiner 是一个**可选的本地 Reducer**，在 Spill 写磁盘前（或 Merge 阶段）对数据进行预聚合，减少写入磁盘和网络传输的数据量。

**使用场景：** 仅适用于满足**结合律和交换律**的操作（如求和、计数、求最大/最小值）。

```
不使用 Combiner：
Map 输出: <apple,1> <apple,1> <apple,1> <banana,1> <banana,1>
  ──► 写入磁盘并传输 5 条记录

使用 Combiner：
Map 本地预聚合: <apple,3> <banana,2>
  ──► 写入磁盘并传输 2 条记录（节省 60% 数据量）
```

**Combiner 触发时机：**

```
溢写时（sort 之后，写磁盘之前）
         │
         ▼
  [Combiner 可选运行]
         │
         ▼
  Spill 文件（已预聚合）

Merge 时（多个 spill 合并）
         │
         ▼
  [Combiner 可选再次运行]
         │
         ▼
  最终 Map 输出文件
```

> ⚠️ **注意**：Combiner 的输出格式必须与 Reducer 的输入格式相同（即 `<K2, V2>`），且其逻辑必须不影响最终结果的正确性。例如，**求平均值不能使用 Combiner**（局部均值的均值 ≠ 全局均值）。

### 4.6 Merge（归并）

Map Task 结束前，会将所有 Spill 文件进行**多路归并（Merge）**，生成一个最终的、已分区且已排序的 Map 输出文件。

**归并策略：**

```
spill0.out  spill1.out  spill2.out  ...  spillN.out
     │            │            │                │
     └────────────┴────────────┴────────────────┘
                          │
                   多路归并排序
                   (merge factor 默认 10)
                          │
                          ▼
              map_output_final.out（已分区+已排序）
            + map_output_final.out.index（分区索引）
```

| 参数 | 配置项 | 默认值 |
|------|--------|--------|
| 归并因子 | `mapreduce.task.io.sort.factor` | 10 |

- 若 Spill 文件数超过归并因子，则分多轮归并。
- 最后一次归并可直接将数据送入 Reduce，**无需写磁盘**（减少一次 I/O）。

**Reduce 端的 Shuffle（Fetch + Merge）：**

Reducer 通过 HTTP 从各个 Map Task 的输出文件中**拉取（Fetch）**属于自己分区的数据：

```
Map 1 输出（partition 1）  ──┐
Map 2 输出（partition 1）  ──┤  HTTP Fetch（并行拉取）
Map 3 输出（partition 1）  ──┤
...                         ──┘
                               │
                               ▼
                    Reduce 端内存缓冲区
                    （mapreduce.reduce.shuffle.input.buffer.percent）
                               │  达到阈值，溢写到磁盘
                               ▼
                    本地 Spill 文件（Reduce 端）
                               │
                    多路归并（Merge）
                               │
                               ▼
                    按 key 分组的排序数据 ──► Reducer 输入
```

| 参数 | 配置项 | 默认值 |
|------|--------|--------|
| Reduce 端内存缓冲比例 | `mapreduce.reduce.shuffle.input.buffer.percent` | 0.70 |
| 并行 Fetch 线程数 | `mapreduce.reduce.shuffle.parallelcopies` | 5 |

---

## 5. Reduce 阶段

Reducer 接收来自所有 Map Task 的、属于该分区的数据，这些数据已经按 key 排序并分组。

### 5.1 GroupingComparator（分组）

归并完成后，框架使用 `GroupingComparator` 将**相邻且相同的 key** 聚合为一组，对用户呈现为 `<key, Iterable<values>>` 的迭代器。

```
归并后的有序数据：
<apple, 1> <apple, 1> <apple, 1> <banana, 1> <banana, 1>
        │                               │
        ▼ GroupingComparator             ▼
<apple, [1, 1, 1]>              <banana, [1, 1]>
        │                               │
        ▼ reduce()                      ▼ reduce()
   <apple, 3>                      <banana, 2>
```

### 5.2 Reducer 执行

用户实现 `reduce()` 函数，对每个 key 的 value 集合进行聚合：

```java
// WordCount 的 Reducer 示例
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
        // 输出：<"apple", 3>, <"banana", 2>, ...
    }
}
```

### 5.3 OutputFormat & RecordWriter（输出写入）

Reducer 的输出通过 `OutputFormat` 写入 HDFS：

| 格式 | 描述 |
|------|------|
| `TextOutputFormat` | 默认格式，每行写 `key\tvalue` |
| `SequenceFileOutputFormat` | 二进制格式，可作为下一个 MR Job 的输入 |
| `NullOutputFormat` | 丢弃输出（仅用于副作用操作） |
| 自定义 `OutputFormat` | 写入数据库、HBase 等 |

每个 Reduce Task 的输出写入 HDFS 上的 `part-r-{taskid}` 文件。

---

## 6. 完整数据流总览

```
HDFS 输入文件
      │
      ▼
InputFormat.getSplits()
──► [InputSplit 1] [InputSplit 2] ... [InputSplit N]
      │
      ▼  RecordReader 解析
[<k1,v1>, <k2,v2>, ...]
      │
      ▼  Mapper.map()
[<k2,v2>, ...]  中间键值对
      │
      ▼  写入环形缓冲区（100MB）
      │
      │  达到 80% 触发溢写
      ▼
  ┌──────────────────────────────────┐
  │  内存排序：按 (partition, key) 排序 │
  │  可选：Combiner 预聚合              │
  └──────────────────────────────────┘
      │
      ▼
  Spill 文件（可能有多个）
      │
      ▼  多路归并（Merge）
  Map 最终输出（已分区+已排序）
      │
      │  HTTP Fetch（Shuffle 网络传输）
      ▼
  Reducer 从所有 Map 拉取自己的分区数据
      │
      ▼  Reduce 端 Merge（内存+磁盘）
  按 key 排好序的数据流
      │
      ▼  GroupingComparator 分组
  <key, Iterable<values>>
      │
      ▼  Reducer.reduce()
  最终输出 <k3, v3>
      │
      ▼  OutputFormat.RecordWriter
HDFS 输出文件（part-r-00000, part-r-00001, ...）
```

---

## 7. 关键参数调优

以下是影响 MapReduce 性能的核心参数，供调优参考：

### Map 端参数

| 参数 | 说明 | 默认值 | 调优建议 |
|------|------|--------|---------|
| `mapreduce.task.io.sort.mb` | Map 端环形缓冲区大小 | 100 MB | 内存充足时可调大至 200-400 MB，减少 Spill 次数 |
| `mapreduce.map.sort.spill.percent` | 触发溢写的阈值 | 0.80 | 通常无需调整 |
| `mapreduce.task.io.sort.factor` | 归并因子（同时归并的文件数） | 10 | 可调大至 100，减少归并轮次 |

### Reduce 端参数

| 参数 | 说明 | 默认值 | 调优建议 |
|------|------|--------|---------|
| `mapreduce.reduce.shuffle.parallelcopies` | 并行 Fetch 线程数 | 5 | Map 数量多时可调大至 20-50 |
| `mapreduce.reduce.shuffle.input.buffer.percent` | Reduce 端 Shuffle 内存比例 | 0.70 | 可适当调大 |
| `mapreduce.reduce.input.buffer.percent` | Reduce 阶段内存中保留数据的比例 | 0.0 | 内存充足时调大，减少磁盘 I/O |

### 压缩设置

启用 Map 输出压缩可显著减少网络传输和磁盘 I/O：

```xml
<!-- 启用 Map 输出压缩 -->
<property>
  <name>mapreduce.map.output.compress</name>
  <value>true</value>
</property>
<property>
  <name>mapreduce.map.output.compress.codec</name>
  <value>org.apache.hadoop.io.compress.SnappyCodec</value>
</property>
```

> **推荐压缩算法**：Snappy（速度快，CPU 开销低）；若需要 Splittable，使用 LZO 或 BZip2。

---

## 8. 总结

| 阶段 | 核心操作 | 数据位置 | 关键点 |
|------|----------|----------|--------|
| **InputSplit** | 逻辑切分文件 | HDFS | 与 Block 对齐，实现数据本地化 |
| **RecordReader** | 解析字节流为 K/V | HDFS → Map 内存 | 行边界对齐，格式可扩展 |
| **Map** | 用户自定义转换 | Map 内存 | 输出写入环形缓冲区 |
| **Partition** | 决定数据去向 | Map 内存（环形缓冲区） | Hash(key) % numReducers |
| **Sort** | 内存内快速排序 | Map 内存 | 按 (partition, key) 双键排序 |
| **Spill** | 内存 → 磁盘 | Map 本地磁盘 | 异步，与 Map 并行 |
| **Combiner** | 本地预聚合（可选） | Map 本地磁盘 | 减少网络传输，需满足结合律 |
| **Merge（Map端）** | 多 Spill 文件归并 | Map 本地磁盘 | 最后一轮可不写磁盘 |
| **Fetch** | HTTP 拉取 Map 输出 | 网络 → Reduce 内存/磁盘 | 并行拉取，可压缩 |
| **Merge（Reduce端）** | 归并所有 Map 输出 | Reduce 内存/磁盘 | 按 key 排序，为分组准备 |
| **Reduce** | 用户自定义聚合 | Reduce 内存 | 接收 `<key, Iterable<values>>` |
| **OutputFormat** | 写入 HDFS | HDFS | 每个 Reducer 输出一个文件 |

MapReduce 的设计哲学是：**在廉价硬件集群上，通过数据本地化、流水线处理和批量 I/O 来实现高吞吐的大规模数据处理**。理解 Shuffle 阶段是优化 MapReduce 作业性能的关键——大多数性能问题（数据倾斜、过多 Spill、网络瓶颈）都源于此。

---

*参考资料：*
- *MapReduce: Simplified Data Processing on Large Clusters* — Jeffrey Dean & Sanjay Ghemawat (Google, 2004)
- Apache Hadoop 官方文档：https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
- *Hadoop: The Definitive Guide* — Tom White (O'Reilly)