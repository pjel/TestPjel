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

public class FilterListExample {
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

            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("dv"), Bytes.toBytes("Motor"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    new SubstringComparator("oli"));
            filter1.setFilterIfMissing(true);

            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("dv"), Bytes.toBytes("Color"),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("Gris"));
            filter1.setFilterIfMissing(true);

            List<Filter> filters = new ArrayList<Filter>();

            filters.add(filter1);
            filters.add(filter2);

            FilterList filterList1 = new FilterList(filters);

            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner scanner1 = tbl.getScanner(scan);
            System.out.println("MUST_PASS_ALL:");
            int n = 0;
            for (Result result : scanner1) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                    n++;
                }
            }
            scanner1.close();
            System.out.println("n: " + n+ "\n");




            FilterList filterList2 = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);

            scan.setFilter(filterList2);
            ResultScanner scanner2 = tbl.getScanner(scan);

            n = 0;
            System.out.println("MUST_PASS_ONE:");
            for (Result result : scanner2) {
                for (Cell cell : result.rawCells()) {
                    System.out.println("Cell: " + cell + ", Value: " +
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                    cell.getValueLength()));
                    n++;
                }
            }
            scanner2.close();
            System.out.println("n: " + n+ "\n");


            tbl.close();
            connection.close();


        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
