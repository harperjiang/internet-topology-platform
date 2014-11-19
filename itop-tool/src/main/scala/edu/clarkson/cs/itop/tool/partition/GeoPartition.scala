package edu.clarkson.cs.itop.tool.partition

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object GeoPartition extends App {
  var conf = new Configuration();
  var job = Job.getInstance(conf, "Geo Partition");
  job.setJarByClass(GeoPartition.getClass);
  job.setMapperClass(classOf[GeoPartitionMapper]);
  job.setOutputKeyClass(classOf[IntWritable]);
  job.setOutputValueClass(classOf[IntWritable]);
  //  FileInputFormat.addInputPath(job, new Path(args(0)));
  FileInputFormat.addInputPath(job, new Path("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/kapar-midar-iff.nodes.geo"))
  //  FileOutputFormat.setOutputPath(job, new Path(args(1)));
  FileOutputFormat.setOutputPath(job, new Path("/home/harper/caida_data/topo-data.caida.org/ITDK/ITDK-2014-04/output/geo_partition"));

  job.waitForCompletion(true);
}

class GeoPartitionMapper extends Mapper[Object, Text, IntWritable, IntWritable] {

  override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
    if (value.toString().startsWith("#"))
      return ;
    var data = value.toString().split("\\s+");
    var id = data(1).substring(1, data(1).length() - 1).toInt;
    var partition = CountryCode.continent(data(3));
    context.write(new IntWritable(id), new IntWritable(partition));
  }
}