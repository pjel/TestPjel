package mibd17.icai.com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class GetList {
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
            byte[] cf1 = Bytes.toBytes("dv");
            byte[] cf2 = Bytes.toBytes("dp");
            byte[] qf1 = Bytes.toBytes("Color");
            byte[] qf2 = Bytes.toBytes("Modelo");
            byte[] qf3 = Bytes.toBytes("Matricula");
            byte[] qf4 = Bytes.toBytes("Motor");
            byte[] qf5 = Bytes.toBytes("Nombre");
            byte[] row1 = Bytes.toBytes("0001");
            byte[] row2 = Bytes.toBytes("0004");

            List<Get> gets = new ArrayList<Get>();


            //Recuperamos la tabla
            Table tbl = connection.getTable(table);


            Get get1 = new Get(row1);
            get1.addColumn(cf1, qf1);
            gets.add(get1);

            Get get2 = new Get(row2);
            get2.addColumn(cf2, qf5);
            gets.add(get2);

            Get get3 = new Get(row2);
            get3.addColumn(cf1, qf3);
            gets.add(get3);

            Result[] results = tbl.get(gets);

            System.out.println("First iteration...");
            for (Result result : results) {
                String row = Bytes.toString(result.getRow());
                System.out.print("Row: " + row + " ");
                byte[] val = null;
                if (result.containsColumn(cf1, qf1)) {
                    val = result.getValue(cf1, qf1);
                    System.out.println("Value: " + Bytes.toString(val));
                }
                if (result.containsColumn(cf2, qf5)) {
                    val = result.getValue(cf2, qf5);
                    System.out.println("Value: " + Bytes.toString(val));
                }
                if (result.containsColumn(cf1, qf3)) {
                    val = result.getValue(cf1, qf3);
                    System.out.println("Value: " + Bytes.toString(val));
                }
            }


            System.out.println("Second iteration...");
            for (Result result : results) {
                if(!result.isEmpty()) {
                    for (Cell cell : result.listCells()) {
                        System.out.println(
                                "Row: " + Bytes.toString(
                                        cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) +
                                        " Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
            }


            System.out.println("Third iteration...");
            for (Result result : results) {
                System.out.println(result);
            }



            tbl.close();
            connection.close();



        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }

        System.out.println("FIN");
    }
}
