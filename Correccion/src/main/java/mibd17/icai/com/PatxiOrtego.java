package mibd17.icai.com;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class PatxiOrtego {

    public static void main(String [] args){
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

            Scan scan1 = new Scan();
            Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,  new BinaryComparator(Bytes.toBytes("0004")));
            scan1.setFilter(filter1);

            ResultScanner scanner1 = tbl.getScanner(scan1);

            System.out.println("");
            for (Result res : scanner1) {
                System.out.println(res);
            }
            scanner1.close();

            Scan scan2 = new Scan();
            Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("00\\d1$"));
            scan2.setFilter(filter2);
            ResultScanner scanner2 = tbl.getScanner(scan2);
            System.out.println("");
            for (Result res : scanner2) {
                System.out.println(res);
            }
            scanner2.close();

            Scan scan3 = new Scan();
            Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("01"));
            scan3.setFilter(filter3);
            ResultScanner scanner3 = tbl.getScanner(scan3);
            System.out.println("");
            for (Result res : scanner3) {
                System.out.println(res);
            }
            scanner3.close();

            tbl.close();
            connection.close();

            Scan scan4 = new Scan();
            Filter filter4 = new RowFilter(CompareFilter.CompareOp.GREATER,  new BinaryComparator(Bytes.toBytes("0004")));
            scan4.setFilter(filter4);

            ResultScanner scanner4 = tbl.getScanner(scan4);

            Scan scan5 = new Scan();
            Filter filter5 = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("01"));
            scan5.setFilter(filter5);
            ResultScanner scanner5 = tbl.getScanner(scan5);
            System.out.println("");
            for (Result res : scanner5) {
                System.out.println(res);
            }
            scanner5.close();

            System.out.println("");
            for (Result res : scanner5) {
                System.out.println(res);
            }
            scanner5.close();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
