package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class AlejandroLopez {
    public static void main(String [] args){
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));

        Connection connection;
        Admin adm = null;

        try{
            connection = ConnectionFactory.createConnection(conf);
            adm = connection.getAdmin();

            String tableName = "AlfSpace:Tabla1Alo";
            TableName table = TableName.valueOf(tableName);

            if(!adm.tableExists(table)){
                System.exit(1);
            }

            //Recuperamos la tabla
            Table tbl = connection.getTable(table);

            //Obtener la última versión de todas las filas
            System.out.println("Table:");
            Scan scan_total = new Scan();
            ResultScanner scanner_total = tbl.getScanner(scan_total);

            for (Result res : scanner_total) {
                System.out.println(res);
            }
            scanner_total.close();

            //Obtener los modelos que cumplan que su motor es Hibrido
            System.out.println("Modelos de los coches con motor Hibrido:");
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("dv"),
                    Bytes.toBytes("Motor"),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("ibri"));
            filter.setFilterIfMissing(true);

            Filter filter2 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,  new BinaryComparator(Bytes.toBytes("0003")));


            List<Filter> filters = new ArrayList<Filter>();
            filters.add(filter);
            filters.add(filter2);
            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();

            scan.addColumn(Bytes.toBytes("dv"),Bytes.toBytes("Modelo"));
            scan.addColumn(Bytes.toBytes("dv"),Bytes.toBytes("Motor"));
            scan.setFilter(filterList1);


            ResultScanner scanner = tbl.getScanner(scan);

            for (Result result : scanner) {

                //Metodo 1
                System.out.println("Metodo 1 ");
                System.out.println("Row: " + Bytes.toString(result.getRow()));

                String rowKey = Bytes.toString(result.getRow());
                String fam = "dv";

                Get get = new Get(Bytes.toBytes(rowKey));
                get.addFamily(Bytes.toBytes(fam));

                Result result2 = tbl.get(get);

                byte[] val1 = result2.getValue(Bytes.toBytes(fam), Bytes.toBytes("Modelo"));
                System.out.println("Modelo: " + Bytes.toString(val1));


                //Metodo 2
                System.out.println("Metodo 2 ");
                List<Cell> cells=result.getColumnCells(Bytes.toBytes("dv"),Bytes.toBytes("Modelo"));

                for(Cell cell: cells){
                    System.out.println("Cell:"+cell + ", Value:"+ Bytes.toString(cell.getValueArray()));
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
