package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class QualifierFilterExample {
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

            Scan scan1 = new Scan();
            Filter filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("matricula")));
            scan1.setFilter(filter);
            ResultScanner scanner = tbl.getScanner(scan1);

            System.out.println("");
            for (Result result : scanner) {
                System.out.println(result);
            }
            scanner.close();

            Get get = new Get(Bytes.toBytes("0002"));
            get.setFilter(filter);
            Result result = tbl.get(get);
            System.out.println("");
            System.out.println("Result of get(): " + result);



            tbl.close();
            connection.close();


        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
