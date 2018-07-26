package mibd17.icai.com;

import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class PutExample {
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
            //Recuperamos la tabla
            Table tbl = connection.getTable(table);

            String rowKey = "0001";
            Put put1 = new Put(Bytes.toBytes(rowKey));
            String fam1 = "dv";
            String qual1 = "Color";
            String val1 = "rojo";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Seat";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "0000-AAA";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Diesel";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 1, Bytes.toBytes(val1));


            String fam2 = "dp";
            String qual2 = "Nombre";
            String val2 = "Juanito Perez";

            put1.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 1, Bytes.toBytes(val2));

            put1.setDurability(Durability.SKIP_WAL);

            tbl.put(put1);


            Put put2 = new Put(Bytes.toBytes(rowKey));
            qual1 = "Motor";
            val1 = "Gasolina";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 2, Bytes.toBytes(val1));

            tbl.put(put2);

            Put put3 = new Put(Bytes.toBytes(rowKey));
            qual1 = "Motor";
            val1 = "Hibrido";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 3, Bytes.toBytes(val1));

            tbl.put(put3);


          /*


            Put put2 = new Put(Bytes.toBytes(rowKey));
            qual = "Modelo";
            val = "Fiat";
            put2.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), Bytes.toBytes(val));

            tbl.put(put2);

            Calendar cal = Calendar.getInstance();
            long ltime = cal.getTimeInMillis();

            rowKey = "0004";
            Put put3 = new Put(Bytes.toBytes(rowKey));
            qual = "Modelo";
            val = "Fiat";
            put3.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual), ltime, Bytes.toBytes(val));

            tbl.put(put3);
*/

            tbl.close();
            connection.close();



        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
