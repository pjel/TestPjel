package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class PutList2 {

    //
    //Cuidado ya que no se garantiza el orden de inserción
    //

    public static void main( String[] args )
    {
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

            List<Put> puts = new ArrayList<Put>();

            String rowKey = "0112";
            Put put1 = new Put(Bytes.toBytes(rowKey));
            String fam1 = "dv";
            String qual1 = "Color";
            String val1 = "Gris";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 4, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Mercedes";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 4, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "1234-AAA";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 4, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Diesel";
            put1.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 4, Bytes.toBytes(val1));


            String fam2 = "dp";
            String qual2 = "Nombre";
            String val2 = "Manolita Lopez";

            put1.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 4, Bytes.toBytes(val2));

            puts.add(put1);



            rowKey = "0213";
            Put put2 = new Put(Bytes.toBytes(rowKey));
            fam1 = "dv";
            qual1 = "Color";
            val1 = "Amarillo";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Volvo";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "1111-ABA";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Diesel";
            put2.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 5, Bytes.toBytes(val1));


            fam2 = "dp";
            qual2 = "Nombre";
            val2 = "Lupita Hernandez";

            put2.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 5, Bytes.toBytes(val2));

            puts.add(put2);





            rowKey = "0154";
            Put put3 = new Put(Bytes.toBytes(rowKey));
            fam1 = "dv";
            qual1 = "Color";
            val1 = "Rojo";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 8, Bytes.toBytes(val1));

            qual1 = "Modelo";
            val1 = "Renault";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 8, Bytes.toBytes(val1));


            qual1 = "Matricula";
            val1 = "1122-ABC";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 8, Bytes.toBytes(val1));

            qual1 = "Motor";
            val1 = "Gasolina";
            put3.addColumn(Bytes.toBytes(fam1), Bytes.toBytes(qual1), 8, Bytes.toBytes(val1));


            fam2 = "dp";
            qual2 = "Nombre";
            val2 = "Felipe Pino";

            put3.addColumn(Bytes.toBytes(fam2), Bytes.toBytes(qual2), 8, Bytes.toBytes(val2));

            puts.add(put3);


            rowKey = "0112";
            Put put11 = new Put(Bytes.toBytes(rowKey));
            String fam11 = "dv";
            String qual11 = "Color";
            String val11 = "Gris";
            put11.addColumn(Bytes.toBytes(fam11), Bytes.toBytes(qual11), 10, Bytes.toBytes(val11));

            puts.add(put11);




            tbl.put(puts);

            tbl.close();
            connection.close();



        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
