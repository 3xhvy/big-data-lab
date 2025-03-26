# Hadoop MapReduce Web Log Analysis

A Hadoop MapReduce project for analyzing web logs and processing data with distributed computing techniques.

## Project Overview

This project demonstrates the use of Hadoop MapReduce to analyze web log data. It provides two main applications:

1. **Web Log Analysis** - Analyzes web logs to count visits to URLs
2. **Character Count** - Counts character frequency in text files

## Technical Requirements

- Java 8+
- Apache Hadoop 3.2.1
- Apache Maven

## Project Structure

```
hadoop-spark-project/
├── input/               # Input data files
├── output/              # Output directory for results
├── src/
│   └── main/
│       └── java/
│           └── org/
│               └── example/
│                   ├── weblog/           # Web log analysis classes
│                   │   ├── WebLogAnalysis.java
│                   │   ├── WebLogMapper.java
│                   │   ├── WebLogReducer.java
│                   │   └── WebLogCombiner.java
│                   └── wordcount/         # Character count classes
│                       └── CharacterCount.java
└── pom.xml             # Maven configuration
```

## MapReduce Components

- **Mapper**: Processes input records, extracts URLs and their visit counts
- **Combiner**: Performs local aggregation to optimize network transfer
- **Reducer**: Aggregates and summarizes the final results

## Building the Project

```bash
# Navigate to project directory
cd hadoop-spark-project

# Build the project with Maven
mvn clean package
```

## Running on Hadoop

### Single-Node Mode (Local)

```bash
# Run the Web Log Analysis
hadoop jar target/hadoop-spark-project-1.0-SNAPSHOT.jar org.example.weblog.WebLogAnalysis input/lab1_data output/weblog_results

# Run the Character Count
hadoop jar target/hadoop-spark-project-1.0-SNAPSHOT.jar org.example.wordcount.CharacterCount input/input.txt output/char_count
```

### Cluster Mode

To run on a Hadoop cluster:

1. Upload your JAR file to the cluster
2. Ensure input files are stored in HDFS
3. Run the MapReduce job:

```bash
# Upload data to HDFS
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put input/lab1_data /user/hadoop/input/

# Run the job on the cluster
hadoop jar hadoop-spark-project-1.0-SNAPSHOT.jar org.example.weblog.WebLogAnalysis /user/hadoop/input/lab1_data /user/hadoop/output/weblog_results
```

## Viewing Results

```bash
# View the results from local file system
cat output/weblog_results/part-r-00000

# View the results from HDFS
hdfs dfs -cat /user/hadoop/output/weblog_results/part-r-00000
```

## Data Format

The web log data is expected to be in CSV format with each line containing:
- URL (string)
- Visit count (integer)

Example:
```
/home, 150
/about, 75
/contact, 45
```