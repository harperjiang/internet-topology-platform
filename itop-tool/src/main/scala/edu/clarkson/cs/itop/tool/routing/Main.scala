package edu.clarkson.cs.itop.tool.routing

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.io.IntWritable

object Main extends App {
  var conf = new Configuration();

  var fs = FileSystem.get(conf);
  // true stands for recursively deleting the folder you gave
  fs.delete(new Path(Config.file("routing")), true);

  var job = Job.getInstance(conf, "Node Partition Join - Geo");
  job.setMapperClass(classOf[NodePartitionJoinMapper]);
  job.setReducerClass(classOf[NodePartitionJoinReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[StringArrayWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[Text]);
  job.setPartitionerClass(classOf[KeyPartitioner]);
  job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/link_partition")))
  FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_raw_geo")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition Filter - Geo")
  job.setMapperClass(classOf[NodePartitionFilterMapper])
  job.setReducerClass(classOf[NodePartitionFilterReducer])
  job.setMapOutputKeyClass(classOf[IntWritable])
  job.setMapOutputValueClass(classOf[IntWritable])
  job.setOutputKeyClass(classOf[IntWritable])
  job.setOutputValueClass(classOf[Text])
  FileInputFormat.addInputPath(job, new Path(Config.file("routing/node_partition_raw_geo")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_geo")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition Join - Random");
  job.setMapperClass(classOf[NodePartitionJoinMapper]);
  job.setReducerClass(classOf[NodePartitionJoinReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[StringArrayWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[Text]);
  job.setPartitionerClass(classOf[KeyPartitioner]);
  job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("random/link_partition")))
  FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_raw_random")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition Filter - Random")
  job.setMapperClass(classOf[NodePartitionFilterMapper])
  job.setReducerClass(classOf[NodePartitionFilterReducer])
  job.setMapOutputKeyClass(classOf[IntWritable])
  job.setMapOutputValueClass(classOf[IntWritable])
  job.setOutputKeyClass(classOf[IntWritable])
  job.setOutputValueClass(classOf[Text])
  FileInputFormat.addInputPath(job, new Path(Config.file("routing/node_partition_raw_random")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_random")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition Join - Degree 1");
  job.setMapperClass(classOf[NodePartitionJoinMapper]);
  job.setReducerClass(classOf[NodePartitionJoinReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[StringArrayWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[Text]);
  job.setPartitionerClass(classOf[KeyPartitioner]);
  job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("degree/link_partition")))
  FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_raw_degree1")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition Filter - Degree 1")
  job.setMapperClass(classOf[NodePartitionFilterMapper])
  job.setReducerClass(classOf[NodePartitionFilterReducer])
  job.setMapOutputKeyClass(classOf[IntWritable])
  job.setMapOutputValueClass(classOf[IntWritable])
  job.setOutputKeyClass(classOf[IntWritable])
  job.setOutputValueClass(classOf[Text])
  FileInputFormat.addInputPath(job, new Path(Config.file("routing/node_partition_raw_degree1")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_degree1")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition Join - Degree n");
  job.setMapperClass(classOf[NodePartitionJoinMapper]);
  job.setReducerClass(classOf[NodePartitionJoinReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[StringArrayWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[Text]);
  job.setPartitionerClass(classOf[KeyPartitioner]);
  job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("degreen/link_partition")))
  FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_raw_degreen")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition Filter - Degree n")
  job.setMapperClass(classOf[NodePartitionFilterMapper])
  job.setReducerClass(classOf[NodePartitionFilterReducer])
  job.setMapOutputKeyClass(classOf[IntWritable])
  job.setMapOutputValueClass(classOf[IntWritable])
  job.setOutputKeyClass(classOf[IntWritable])
  job.setOutputValueClass(classOf[Text])
  FileInputFormat.addInputPath(job, new Path(Config.file("routing/node_partition_raw_degreen")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("routing/node_partition_degreen")))
  job.waitForCompletion(true);
}