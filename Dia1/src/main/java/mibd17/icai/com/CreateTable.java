package mibd17.icai.com;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;


public class CreateTable {
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

            //Cambiar por cada alumno
            String tableName = "PjelSpace:Tabla1Pema";
            TableName table = TableName.valueOf(tableName);
            TableName table2 = TableName.valueOf("PjelSpace", "Tabla1Pema");


            //Se crea la definición de la tabla
            HTableDescriptor desc = new HTableDescriptor(table);
            //desc.setDurability(Durability.SKIP_WAL);

            //Se añade la definición de la CF1 datos del vehículo
            HColumnDescriptor coldef1 = new HColumnDescriptor("dv");
            coldef1.setMaxVersions(2);
            //coldef1.setPrefetchBlocksOnOpen(true);
            desc.addFamily(coldef1);

            //Se añade la definición de la CF2 datos del propietario
            HColumnDescriptor coldef2 = new HColumnDescriptor("dp");
            coldef2.setMaxVersions(3);
            desc.addFamily(coldef2);

            //Se crea la tabla
            adm.createTable(desc);

            connection.close();


        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
