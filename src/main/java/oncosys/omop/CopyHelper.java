package oncosys.omop;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import scala.collection.Seq;

public class CopyHelper extends InputStream {
    private final Iterator<Row> rows;
    private Iterator<Byte> bytes;

    public CopyHelper(Iterator<Row> rows) {
        this.rows = rows;
        this.bytes = Collections.emptyIterator();
    }

    @Override
    public int read() {
        if (bytes.hasNext()) {
            return bytes.next() & 0xff;
        } else {
            if (rows.hasNext()) {
                final Seq<Object> row = rows.next().toSeq();
                final List<String> strings = new ArrayList<>(row.size());
                final scala.collection.Iterator<Object> it = row.iterator();
                while (it.hasNext()) {
                    final Object val = it.next();
                    if (val == null) {
                        strings.add("\\N");
                    } else {
                        strings.add("\"" + val.toString().replaceAll("\"", "\"\"") + "\"");
                    }
                    final byte[] temp_bytes = (String.join("\t", strings) + "\n").getBytes();
                    final Byte[] boxed_temp_bytes = new Byte[temp_bytes.length];
                    for (int i = 0; i < temp_bytes.length; i++) boxed_temp_bytes[i] = temp_bytes[i];
                    this.bytes = Arrays.stream(boxed_temp_bytes).iterator();
                }
                return bytes.next() & 0xff;
            } else {
                return -1;
            }
        }
    }

    public static void copyIn(Dataset<Row> df, String table, String url, String user, String password) throws ExecutionException, InterruptedException {
        df.toJavaRDD().foreachPartitionAsync(rows -> {
            try (Connection conn = DriverManager.getConnection(url, user, password)) {
                CopyManager cm = new CopyManager((BaseConnection) conn);
                cm.copyIn(String.format("COPY %s FROM STDIN WITH (NULL '\\N', FORMAT CSV, DELIMITER E'\\t')", table), new CopyHelper(rows));
            }
        }).get();
    }
}