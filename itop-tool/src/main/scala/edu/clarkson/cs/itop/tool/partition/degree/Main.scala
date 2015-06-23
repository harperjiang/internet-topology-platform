package edu.clarkson.cs.itop.tool.partition.degree

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import edu.clarkson.cs.itop.tool.types.KeyGroupComparator
import edu.clarkson.cs.itop.tool.types.KeyPartitioner
import edu.clarkson.cs.itop.tool.types.StringArrayWritable
import org.apache.hadoop.fs.FileSystem

object Main extends App {

  runJob;

  def runJob = {
    var conf = new Configuration();

    FileSystem.get(conf).delete(new Path(Config.file("degree")), true);

    var icjob1 = Job.getInstance(conf, "Degree 1 - Join Link Degree");
    icjob1.setJarByClass(Main.getClass);
    icjob1.setMapperClass(classOf[JoinLinkDegreeMapper]);
    icjob1.setReducerClass(classOf[JoinLinkDegreeReducer]);
    icjob1.setMapOutputKeyClass(classOf[StringArrayWritable]);
    icjob1.setMapOutputValueClass(classOf[StringArrayWritable]);
    icjob1.setOutputKeyClass(classOf[IntWritable]);
    icjob1.setOutputValueClass(classOf[Text]);
    icjob1.setPartitionerClass(classOf[KeyPartitioner]);
    icjob1.setGroupingComparatorClass(classOf[KeyGroupComparator]);
    FileInputFormat.addInputPath(icjob1, new Path(Config.file("common/node_link")));
    FileInputFormat.addInputPath(icjob1, new Path(Config.file("common/node_degree")));
    FileOutputFormat.setOutputPath(icjob1, new Path(Config.file("degree/link_degree")));
    icjob1.waitForCompletion(true);

    var icjob2 = Job.getInstance(conf, "Degree 1 - Max Degree");
    icjob2.setJarByClass(Main.getClass);
    icjob2.setMapperClass(classOf[MaxDegreeMapper]);
    icjob2.setReducerClass(classOf[MaxDegreeReducer]);
    icjob2.setOutputKeyClass(classOf[IntWritable]);
    icjob2.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(icjob2, new Path(Config.file("degree/link_degree")))
    FileOutputFormat.setOutputPath(icjob2, new Path(Config.file("degree/max_degree")));
    icjob2.waitForCompletion(true);

    var pljob = Job.getInstance(conf, "Degree 1 - Partition");
    pljob.setJarByClass(Main.getClass);
    pljob.setMapperClass(classOf[PartitionLinkMapper]);
    pljob.setOutputKeyClass(classOf[IntWritable]);
    pljob.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(pljob, new Path(Config.file("degree/max_degree")))
    FileOutputFormat.setOutputPath(pljob, new Path(Config.file("degree/link_partition")));
    pljob.waitForCompletion(true);
  }

}

