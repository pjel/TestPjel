package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class PageFilterExample {
    private static final byte[] POSTFIX = new byte[] { 0x00 };

    public static void main(String [] args){
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));

        Connection connection;
        Admin adm = null;

        try{
            connection = ConnectionFactory.createConnection(conf);
            conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
            conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));

            adm = connection.getAdmin();

            String tableName = "PjelSpace:Tabla1Pema";
            TableName table = TableName.valueOf(tableName);

            if(!adm.tableExists(table)){
                System.exit(1);
            }

            //Recuperamos la tabla
            Table tbl = connection.getTable(table);


            Filter filter = new PageFilter(3);


            int totalRows = 0;
            byte[] lastRow = null;
            while (true) {
                Scan scan = new Scan();
                scan.setFilter(filter);
                if (lastRow != null) {
                    byte[] startRow = Bytes.add(lastRow, POSTFIX);
                    System.out.println("start row: " +
                            Bytes.toStringBinary(startRow));
                    scan.setStartRow(startRow);
                }
                ResultScanner scanner = tbl.getScanner(scan);
                int localRows = 0;
                Result result;
                while ((result = scanner.next()) != null) {
                    System.out.println(localRows++ + ": " + result);
                    totalRows++;
                    lastRow = result.getRow();
                }
                scanner.close();
                if (localRows == 0) break;
            }
            System.out.println("total rows: " + totalRows);


            tbl.close();
            connection.close();


        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
