import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable{

	Text tag;
	int nb;
	
	public StringAndInt(Text s,int i){
		tag =s;
		nb = i;
	}
	
	public StringAndInt(){
		tag = new Text();
		nb = 0;
	}
	
	@Override
	public int compareTo(StringAndInt o) { 
		return o.nb - this.nb;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nb = in.readInt();
		tag.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(nb);
		tag.write(out);
	}
	
	public static StringAndInt read(DataInput in) throws IOException {
        StringAndInt w = new StringAndInt();
        w.readFields(in);
        return w;
      }
	
}