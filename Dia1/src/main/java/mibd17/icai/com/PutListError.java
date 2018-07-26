package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class PutListError {
    public static void main( String[] args )
    {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));




        Connection connection = null;
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


            List<Put> puts = new ArrayList<Put>();

            String rowKey = "0005";
            Put put1 = new Put(Bytes.toBytes(rowKey));
            String fam1 = "dv";
            String qual1 = "Color";
            String val1 = "Negro";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Toyota";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 6, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "5656-CBA";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 7, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Gasolina";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 8, Bytes.toBytes(val1));


            String fam2 = "dp";
            String qual2 = "Nombre";
            String val2 = "Santiago Bernabeu";

            put1.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 9, Bytes.toBytes(val2));

            puts.add(put1);



            rowKey = "0006";
            Put put2 = new Put(Bytes.toBytes(rowKey));
            fam1 = "FamEROR";
            qual1 = "Color";
            val1 = "Blanco";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 10, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Skoda";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 11, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "1111-BBB";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 12, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Diesel";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 15, Bytes.toBytes(val1));


            fam2 = "dp";
            qual2 = "Nombre";
            val2 = "Lupita Hernandez";

            put2.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 14, Bytes.toBytes(val2));

            puts.add(put2);





            rowKey = "0004";
            Put put3 = new Put(Bytes.toBytes(rowKey));
            fam1 = "dv";
            qual1 = "Color";
            val1 = "Azul";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 15, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Renault";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 16, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "1122-ABC";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 17, Bytes.toBytes(val1));


            fam2 = "dp";
            qual2 = "Nombre";
            val2 = "Felipe Masa";

            put3.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 18, Bytes.toBytes(val2));

            puts.add(put3);




            try {
                tbl.put(puts);
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
