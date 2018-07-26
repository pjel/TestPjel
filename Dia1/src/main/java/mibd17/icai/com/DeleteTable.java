package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DeleteTable {
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
            tableName ="PjelSpace:Tabla1PemaPre2";
            TableName table = TableName.valueOf(tableName);

            if(adm.tableExists(table)){
                //Para poder borrar una tabla tiene que estar deshabilitada
                if (adm.isTableEnabled(table)){
                    adm.disableTable(table);
                }
                //Se borra la tabla
                adm.deleteTable(table);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");

        }
}
