package edu.clarkson.cs.itop.tool.partition.degreen

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import edu.clarkson.cs.itop.tool.Config
import org.apache.hadoop.fs.FileSystem
import edu.clarkson.cs.itop.tool.Param

object Main extends App {

  var conf = new Configuration();
  FileSystem.get(conf).delete(new Path(Config.file("degreen")), true);

  initTriple();
  for (i <- 0 to Param.degree_n - 1) {
    adjustTriple();
  }
  assignTriplePartition();

  def initTriple() = {
    var job = Job.getInstance(conf, "Degree - Init Triple");
    job.setJarByClass(Main.getClass);
    job.setMapperClass(classOf[InitTripleMapper]);
    job.setMapOutputKeyClass(classOf[Text]);
    job.setMapOutputValueClass(classOf[Text]);
    job.setOutputKeyClass(classOf[Text]);
    job.setOutputValueClass(classOf[Text]);
    FileInputFormat.addInputPath(job, new Path(Config.file("common/node_degree")));
    FileOutputFormat.setOutputPath(job, new Path(Config.file("degreen/triple")));
    job.waitForCompletion(true);
  }

  def adjustTriple() = {

  }

  def assignTriplePartition() = {

  }
}