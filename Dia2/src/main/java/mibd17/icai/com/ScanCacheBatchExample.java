package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanCacheBatchExample {

    private static void scan(Table table, int caching, int batch) throws IOException {
        int count = 0;
        Scan scan = new Scan();
        scan.setCaching(caching);
        scan.setBatch(batch);
        scan.setScanMetricsEnabled(true);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            count++;
        }
        scanner.close();
        ScanMetrics metrics = scan.getScanMetrics();
        System.out.println("Caching: " + caching + ", Batch: " + batch +
                ", Results: " + count +
                ", RPCs: " + metrics.countOfRPCcalls);
    }


    public static void main(String [] args) {
        // Obtenemos la configuración del cluster
        Configuration conf = HBaseConfiguration.create();

        Connection connection;
        Admin adm = null;

        try{
            connection = ConnectionFactory.createConnection(conf);
            adm = connection.getAdmin();

            String tableName = "PjelSpace:Tabla1Pema";
            TableName table = TableName.valueOf(tableName);

            if(!adm.tableExists(table)){
                System.exit(1);
            }

            //Recuperamos la tabla
            Table tbl = connection.getTable(table);

            scan(tbl,1, 1);
            scan(tbl,1, 0);
            scan(tbl,1, 0);
            scan(tbl,2, 1);
            scan(tbl,2, 0);
            scan(tbl,2, 0);
            scan(tbl,2000, 100);


            tbl.close();
            connection.close();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");

    }
}
