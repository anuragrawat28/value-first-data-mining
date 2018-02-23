package value_first_map_reduce1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Driver_code {

   public static void main(String[] args) throws Exception {
       
       Configuration conf = new Configuration();
             Job job = Job.getInstance(conf, "WordCounter");
             job.setJarByClass(Driver_code.class);
             job.setMapperClass(Mapper_code.class);
             //job.setNumReduceTasks(0);
             job.setReducerClass(Reducer_code.class);
       
        
             job.setOutputKeyClass(Text.class);
             job.setOutputValueClass(Text.class);
        
             FileInputFormat.setInputPaths(job, new Path(args[0]));
             FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
             if (!job.waitForCompletion(true))
                return;
          }
   }