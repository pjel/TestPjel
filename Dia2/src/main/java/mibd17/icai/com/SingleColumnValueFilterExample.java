package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleColumnValueFilterExample {
    public static void main(String [] args){
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));

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

            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("dv"),
                    Bytes.toBytes("Motor"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    new SubstringComparator("oli"));
            filter.setFilterIfMissing(true);

            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = tbl.getScanner(scan);
            System.out.println("");
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                }
            }
            scanner.close();


            Scan scan2 = new Scan();
            scan2.addColumn(Bytes.toBytes("dv"),Bytes.toBytes("Matricula"));
            scan.setFilter(filter);
            ResultScanner scanner2 = tbl.getScanner(scan2);
            System.out.println("");
            for (Result result : scanner2) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                }
            }
            scanner.close();

            tbl.close();
            connection.close();


        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
