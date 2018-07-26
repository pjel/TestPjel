package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class FamilyFilterExample {
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

            Scan scan1 = new Scan();
            Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("dv")));
            scan1.setFilter(filter1);
            ResultScanner scanner1 = tbl.getScanner(scan1);
            System.out.println("");

            for (Result result : scanner1) {
                System.out.println(result);
            }
            scanner1.close();


            Get get1 = new Get(Bytes.toBytes("0001"));
            get1.setFilter(filter1);
            Result result1 = tbl.get(get1);
            System.out.println("");
            System.out.println("Result of get(): " + result1);


            Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("dv")));
            Get get2 = new Get(Bytes.toBytes("0004"));
            get2.addFamily(Bytes.toBytes("dp"));
            get2.setFilter(filter2);
            Result result2 = tbl.get(get2);
            System.out.println("");
            System.out.println("Result of get(): " + result2);


            tbl.close();
            connection.close();


        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
