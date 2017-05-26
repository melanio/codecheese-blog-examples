package codecheese.blog.examples.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

/**
 * Modifiedj example from
 * https://orc.apache.org/docs/core-java.html
 *
 * Created by mreyes on 6/5/16.
 */
public class OrcReaderUsingVectorizedRowBatch {
	public static void main(String [ ] args) throws java.io.IOException
	{
		Configuration conf = new Configuration();
		Reader reader = OrcFile.createReader(new Path("/tmp/my-file.orc"),
				OrcFile.readerOptions(conf));

		RecordReader rows = reader.rows();
		VectorizedRowBatch batch = reader.getSchema().createRowBatch();

		while (rows.nextBatch(batch)) {
			LongColumnVector intVector = (LongColumnVector) batch.cols[0];
			LongColumnVector longVector = (LongColumnVector) batch.cols[1];
			DoubleColumnVector doubleVector  = (DoubleColumnVector) batch.cols[2];
			DoubleColumnVector floatVector = (DoubleColumnVector) batch.cols[3];
			LongColumnVector booleanVector = (LongColumnVector) batch.cols[4];
			BytesColumnVector stringVector = (BytesColumnVector)  batch.cols[5];


			for(int r=0; r < batch.size; r++) {
				int intValue = (int) intVector.vector[r];
				long longValue = longVector.vector[r];
				double doubleValue = doubleVector.vector[r];
				double floatValue = (float) floatVector.vector[r];
				boolean boolValue = booleanVector.vector[r] != 0;
				String stringValue = new String(stringVector.vector[r], stringVector.start[r], stringVector.length[r]);

				System.out.println(intValue + ", " + longValue + ", " + doubleValue + ", " + floatValue + ", " + boolValue + ", " + stringValue);

			}
		}
		rows.close();
	}
}
