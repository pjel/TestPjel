package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteExample2 {
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
            //Recuperamos la tabla
            Table tbl = connection.getTable(table);

            String rowKey = "0002";
            String fam1 = "dv";
            String fam2 = "dp";
            String qual = "Color";
            Delete delete1 = new Delete(Bytes.toBytes(rowKey));
            tbl.delete(delete1);

          /*  rowKey = "0003";
            Delete delete2 = new Delete(Bytes.toBytes(rowKey));
            delete2.addFamily(Bytes.toBytes(fam2));
            tbl.delete(delete2);*/

            rowKey = "0003";
            Delete delete3 = new Delete(Bytes.toBytes(rowKey));
            delete3.addColumn(Bytes.toBytes(fam1),Bytes.toBytes(qual));
            delete3.addColumn(Bytes.toBytes(fam1),Bytes.toBytes("Matricula"));
            tbl.delete(delete3);

/*
            rowKey = "0004";
            Delete delete4 = new Delete(Bytes.toBytes(rowKey));
            delete4.addFamily(Bytes.toBytes(fam2),15);
            tbl.delete(delete4);*/

            //No borra por la familia
           /* rowKey = "0001";
            Delete delete5 = new Delete(Bytes.toBytes(rowKey));
            delete5.addColumn(Bytes.toBytes(fam2),Bytes.toBytes("Motor"),4);
            tbl.delete(delete5);*/

            //Atentos a la s de columns
           /* rowKey = "0001";
            Delete delete6 = new Delete(Bytes.toBytes(rowKey));
            delete6.addColumns(Bytes.toBytes(fam1),Bytes.toBytes("Motor"),4);
            tbl.delete(delete6);*/



            tbl.close();
            connection.close();

            // Volver a ejecutar los put y ver que pasa



        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
