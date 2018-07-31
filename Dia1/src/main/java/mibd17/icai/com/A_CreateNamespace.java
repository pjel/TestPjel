package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class A_CreateNamespace {
    public static void main(String[] args) {

        // Obtenemos la configuración del cluster
        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path("/home/icai/tmp/Cloudera/hbase-site.xml"));
//        conf.addResource(new Path("/home/icai/tmp/Cloudera/core-site.xml"));
     //   conf.set("hbase.zookeeper.quorum","master01.bigdata.alumnos.upcont.es,master02.bigdata.alumnos.upcont.es,manager01.bigdata.alumnos.upcont.es");
     //   conf.set("hbase.zookeeper.property.clientPort","2181");




        Connection connection;
        Admin adm = null;

        //Modificar por alumno
        String name = "PjelSpace";

        NamespaceDescriptor namespace = NamespaceDescriptor.create(name).build();
        try {
            connection = ConnectionFactory.createConnection(conf);
            adm = connection.getAdmin();
            //adm.deleteNamespace(name);
            adm.createNamespace(namespace);

            NamespaceDescriptor[] list = adm.listNamespaceDescriptors();
            for (NamespaceDescriptor nd : list) {
                System.out.println("List Namespace: " + nd);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
