package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetListError {
    public static void main( String[] args )
    {
        // Obtenemos la configuración del cluster
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
            byte[] cf1 = Bytes.toBytes("dv");
            byte[] cf2 = Bytes.toBytes("dp");
            byte[] qf1 = Bytes.toBytes("Color");
            byte[] qf2 = Bytes.toBytes("Modelo");
            byte[] qf3 = Bytes.toBytes("Matricula");
            byte[] qf4 = Bytes.toBytes("Motor");
            byte[] qf5 = Bytes.toBytes("Nombre");
            byte[] row1 = Bytes.toBytes("0001");
            byte[] row2 = Bytes.toBytes("0004");

            List<Get> gets = new ArrayList<Get>();


            //Recuperamos la tabla
            Table tbl = connection.getTable(table);


            Get get1 = new Get(row1);
            get1.addColumn(cf1, qf1);
            gets.add(get1);

            Get get2 = new Get(row2);
            get2.addColumn(cf1, qf1);
            gets.add(get2);

            Get get3 = new Get(row2);
            get3.addColumn(cf1, qf2);
            gets.add(get3);

            Get get4 = new Get(row2);
            get4.addColumn(Bytes.toBytes("ERROR"), qf2);
            gets.add(get4);

            Result[] results = new Result[0];
            try {
                results = tbl.get(gets);
            } catch (RetriesExhaustedWithDetailsException e) {
                int numErrors = e.getNumExceptions();
                System.out.println("Number of exceptions: " + numErrors);
                for (int n = 0; n < numErrors; n++) {
                    System.out.println("Cause[" + n + "]: " + e.getCause(n));
                    System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
                    System.out.println("Row[" + n + "]: " + e.getRow(n)); // co PutListErrorExample3-3-ErrorPut Gain access to the failed operation.
                }
                System.out.println("Cluster issues: " + e.mayHaveClusterIssues());
                System.out.println("Description: " + e.getExhaustiveDescription());
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }



            tbl.close();
            connection.close();



        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
