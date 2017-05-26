package codecheese.blog.examples.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * Modifiedj example from
 * https://orc.apache.org/docs/core-java.html
 *
 * Created by mreyes on 6/5/16.
 */
public class SimpleOrcWriterUsingVectorizedRowBatch {
	public static void main(String [ ] args) throws java.io.IOException
	{
		Configuration conf = new Configuration();
		TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
		Writer writer = OrcFile.createWriter(new Path("/tmp/my-file.orc"),
				OrcFile.writerOptions(conf)
						.setSchema(schema));


		VectorizedRowBatch batch = schema.createRowBatch();
		LongColumnVector x = (LongColumnVector) batch.cols[0];
		LongColumnVector y = (LongColumnVector) batch.cols[1];
		for(int r=0; r < 10000; ++r) {
			int row = batch.size++;
			x.vector[row] = r;
			y.vector[row] = r * 3;
			// If the batch is full, write it out and start over.
			if (batch.size == batch.getMaxSize()) {
				writer.addRowBatch(batch);
				batch.reset();
			}
		}
		if (batch.size != 0) {
			writer.addRowBatch(batch);
			batch.reset();
		}
		writer.close();
	}

}
