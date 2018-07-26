package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class PrefixFilterExample {
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

            Filter filter = new PrefixFilter(Bytes.toBytes("001"));

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

            //No devuelve nada ya que busca coincidencia exacta de clave
            Get get = new Get(Bytes.toBytes("001"));
            get.setFilter(filter);
            Result result = tbl.get(get);
            System.out.println("Result of get: ");
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }




            //Lo mismo pero sin filtros

            System.out.println("");
            Scan scan2 = new Scan();
            scan2.setStartRow(Bytes.toBytes("001"))
                    .setStopRow(Bytes.toBytes("002"));
            ResultScanner scanner2 = tbl.getScanner(scan2);
            System.out.println("");
            for (Result result2 : scanner2) {
                for (Cell cell : result2.rawCells()) {
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
