package edu.clarkson.cs.itop.tool.partition.geo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.common.CounterMapper
import edu.clarkson.cs.itop.tool.common.CounterParam
import edu.clarkson.cs.itop.tool.common.CounterReducer
import org.apache.hadoop.io.NullWritable
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator

object Main extends App {

  var conf = new Configuration();

  FileSystem.get(conf).delete(new Path(Config.file("geo")), true);

  var job = Job.getInstance(conf, "Node Geo");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[NodeGeoMapper]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[Text]);
  job.setNumReduceTasks(0);
  FileInputFormat.addInputPath(job, new Path(Config.file("kapar-midar-iff.nodes.geo")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/node_geo")))
  job.waitForCompletion(true);

  conf.set(CounterParam.KEY_INDEX, "1");
  job = Job.getInstance(conf, "Geo Count");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[CounterMapper]);
  job.setReducerClass(classOf[CounterReducer]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/node_geo")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/geo_count")))
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Geo Partition");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[GeoPartitionMapper]);
  job.setReducerClass(classOf[GeoPartitionReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[NullWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[IntWritable]);
  job.setNumReduceTasks(1);
  job.setGroupingComparatorClass(classOf[GeoPartitionGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/geo_count")));
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/geo_partition")));
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Node Partition");
  job.setJarByClass(Main.getClass);
  job.setMapperClass(classOf[NodeGeoJoinMapper]);
  job.setReducerClass(classOf[NodeGeoJoinReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[StringArrayWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[Text]);
  job.setPartitionerClass(classOf[KeyPartitioner]);
  job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/geo_partition")));
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/node_geo")));
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/node_partition")));
  job.waitForCompletion(true);

  job = Job.getInstance(conf, "Link Partition Join");
  job.setMapperClass(classOf[LinkPartitionJoinMapper]);
  job.setReducerClass(classOf[LinkPartitionJoinReducer]);
  job.setMapOutputKeyClass(classOf[StringArrayWritable]);
  job.setMapOutputValueClass(classOf[StringArrayWritable]);
  job.setOutputKeyClass(classOf[Text]);
  job.setOutputValueClass(classOf[Text]);
  job.setPartitionerClass(classOf[KeyPartitioner]);
  job.setGroupingComparatorClass(classOf[KeyGroupComparator]);
  FileInputFormat.addInputPath(job, new Path(Config.file("common/node_link")))
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/node_partition")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/link_partition_join")))
  job.waitForCompletion(true);
  //  
  // assign links to majority partitions of nodes around it, break the tie randomly

  job = Job.getInstance(conf, "Link Assign");
  job.setMapperClass(classOf[LinkAssignMapper]);
  job.setReducerClass(classOf[LinkAssignReducer]);
  job.setMapOutputKeyClass(classOf[IntWritable]);
  job.setMapOutputValueClass(classOf[IntWritable]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  FileInputFormat.addInputPath(job, new Path(Config.file("geo/link_partition_join")))
  FileOutputFormat.setOutputPath(job, new Path(Config.file("geo/link_partition")))
  job.waitForCompletion(true);
}