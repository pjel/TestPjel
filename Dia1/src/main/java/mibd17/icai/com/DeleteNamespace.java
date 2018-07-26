package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;

public class DeleteNamespace {
    public static void main(String [] args){
        // Obtenemos la configuración del cluster
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));

        Connection connection;
        Admin adm = null;

        String name = "PjelSpace";

        try {
            connection = ConnectionFactory.createConnection(conf);
            adm = connection.getAdmin();

            TableName[] tbls = adm.listTableNamesByNamespace(name);
            for (TableName tbl : tbls) {
                if (adm.isTableEnabled(tbl)){
                    adm.disableTable(tbl);
                }
                adm.deleteTable(tbl);
            }
            adm.deleteNamespace(name);

            NamespaceDescriptor[] list = adm.listNamespaceDescriptors();
            for (NamespaceDescriptor nd : list) {
                System.out.println("List Namespace: " + nd);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
