package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class DeleteListError {
    public static void main(String [] args){
        // Obtenemos la configuración del cluster
        Configuration conf = HBaseConfiguration.create();

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
            List<Delete> deletes = new ArrayList<Delete>();

            String rowKey = "0001";
            Delete delete1 = new Delete(Bytes.toBytes(rowKey));
            delete1.addFamily(Bytes.toBytes("FamilyERROR"));
            deletes.add(delete1);

            rowKey = "0003";
            Delete delete2 = new Delete(Bytes.toBytes(rowKey));
            deletes.add(delete2);

            rowKey = "0004";
            Delete delete3 = new Delete(Bytes.toBytes(rowKey));
            deletes.add(delete3);

            rowKey = "0005";
            Delete delete4 = new Delete(Bytes.toBytes(rowKey));
            deletes.add(delete4);

            try {
                tbl.delete(deletes);
            } catch (Exception e) {
                System.err.println("Error: " + e);
            }
            tbl.close();

            System.out.println("Deletes length: " + deletes.size());
            for (Delete delete : deletes) {
                System.out.println(delete);
            }


            connection.close();




        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
