package mibd17.icai.com;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;


public class CreateTablePresplit {
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

            String tableName1 = "PjelSpace:Tabla1PemaPre1";
            TableName table1 = TableName.valueOf(tableName1);
            TableName table11 = TableName.valueOf("PjelSpace", "Tabla1Pema1");

            String tableName2 = "PjelSpace:Tabla1PemaPre2";
            TableName table2 = TableName.valueOf(tableName2);
            TableName table22 = TableName.valueOf("PjelSpace", "Tabla1Pema2");


            //Se crea la definición de la tabla
            HTableDescriptor desc1 = new HTableDescriptor(table1);
            HTableDescriptor desc2 = new HTableDescriptor(table2);

            //desc1.setDurability(Durability.SKIP_WAL);

            byte[][] regions = new byte[][] {
                    Bytes.toBytes("0002"),
                    Bytes.toBytes("0005"),
                    Bytes.toBytes("0007")
            };

            //Se añade la definición de la CF1 datos del vehículo
            HColumnDescriptor coldef1 = new HColumnDescriptor("dv");
            coldef1.setMaxVersions(2);
            desc1.addFamily(coldef1);

            //Se añade la definición de la CF2 datos del propietario
            HColumnDescriptor coldef2 = new HColumnDescriptor("dp");
            coldef2.setMaxVersions(3);
            desc1.addFamily(coldef2);

            //Se crea la tabla
            adm.createTable(desc1,regions);


            HColumnDescriptor coldef3 = new HColumnDescriptor("de");
            coldef3.setMaxVersions(2);
            desc2.addFamily(coldef3);

            adm.createTable(desc2, Bytes.toBytes("0000"), Bytes.toBytes("0020"), 5);

            connection.close();


        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
