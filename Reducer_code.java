package value_first_map_reduce1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer_code extends
      Reducer<Text, Text,Text,Text> {
    

      public void reduce(Text key, Text values, Context context) throws IOException
      {
		  try {
	  			context.write(key, values);
    			 }
  			
  		catch (InterruptedException e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
          
      }
}