package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Calendar;

public class GetExample {
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

            String rowKey1 = "0001";
            Get get1 = new Get(Bytes.toBytes(rowKey1));
            String fam = "dv";
            String qual = "Color";
            get1.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
            // comentar esta linea en segunda ejecución
            get1.addColumn(Bytes.toBytes(fam), Bytes.toBytes("Modelo"));

            Result result1 = tbl.get(get1);

            byte[] val1 = result1.getValue(Bytes.toBytes(fam), Bytes.toBytes(qual));
            byte[] val2 = result1.getValue(Bytes.toBytes(fam), Bytes.toBytes("Modelo"));
            System.out.println("Color: " + Bytes.toString(val1)+ " Modelo: " + Bytes.toString(val2));


            String rowKey2 = "0004";
            Get get2 = new Get(Bytes.toBytes(rowKey2));
            get2.addFamily(Bytes.toBytes(fam));

            Result result2 = tbl.get(get2);

            val1 = result2.getValue(Bytes.toBytes(fam), Bytes.toBytes("Matricula"));
            val2 = result2.getValue(Bytes.toBytes(fam), Bytes.toBytes("Motor"));
            System.out.println("Matricula: " + Bytes.toString(val1)+ " Motor: " + Bytes.toString(val2));

            Get get3 = new Get(Bytes.toBytes(rowKey2));
            Result result3 = tbl.get(get3);
            val1 = result3.getValue(Bytes.toBytes("dp"), Bytes.toBytes("Nombre"));
            val2 = result3.getValue(Bytes.toBytes(fam), Bytes.toBytes("Modelo"));
            System.out.println("Nombre: " + Bytes.toString(val1)+ " Modelo: " + Bytes.toString(val2));

            tbl.close();
            connection.close();



        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
