package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class BatchExample {
    public static void main(String [] args){
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


            List<Row> batch = new ArrayList<Row>();

            if(!adm.tableExists(table)){
                System.exit(1);
            }
            //Recuperamos la tabla
            Table tbl = connection.getTable(table);

            String rowKey = "0012";
            Put put1 = new Put(Bytes.toBytes(rowKey));
            String fam1 = "dv";
            String qual1 = "Color";
            String val1 = "Gris";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 100, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Mercedes";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 100, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "1234-AAA";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 100, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Diesel";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 100, Bytes.toBytes(val1));


            String fam2 = "dp";
            String qual2 = "Nombre";
            String val2 = "Manolita Lopez";

            put1.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 100, Bytes.toBytes(val2));

            batch.add(put1);

            rowKey = "0001";
            Delete delete1 = new Delete(Bytes.toBytes(rowKey));
            batch.add(delete1);

            rowKey = "0006";
            Put put2 = new Put(Bytes.toBytes(rowKey));
            fam1 = "FamEROR";
            qual1 = "Color";
            val1 = "Blanco";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 10, Bytes.toBytes(val1));

            batch.add(put2);


            Object[] results = new Object[batch.size()];
            try {
                tbl.batch(batch, results);
            } catch (Exception e) {
                System.err.println("Error: " + e);
            }

            for (int i = 0; i < results.length; i++) {
                System.out.println("Result[" + i + "]: type = " +
                        results[i].getClass().getSimpleName() + "; " + results[i]);
            }

            tbl.close();
            connection.close();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }


        System.out.println("FIN");
    }

}
