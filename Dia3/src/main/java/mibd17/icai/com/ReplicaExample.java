package mibd17.icai.com;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.UUID;

public class ReplicaExample extends BaseRegionObserver {

    private static final byte[] CF = Bytes.toBytes("detalle");
    private static final byte[] ACCION = Bytes.toBytes("accion");
    private static final byte[] ID = Bytes.toBytes("id");
    private static final byte[] TABLE_NAME = Bytes.toBytes("table");
    private static final byte[] NCOLUMNS = Bytes.toBytes("columnas");

    private static final byte[] TABLE = Bytes.toBytes("PjelSpace:Auditoria");


    private Table audit = null;

    public void start(CoprocessorEnvironment env) throws IOException {
        String tableName = "PjelSpace:Auditoria";
        TableName table = TableName.valueOf(tableName);
        //Configuration conf = env.getConfiguration();
        audit = env.getTable(table);

    }


    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
                         Put put,
                         WALEdit edit,
                         Durability durability)
            throws IOException {

        RegionCoprocessorEnvironment env = e.getEnvironment();


        try {

            String tablename= env.getRegion().getRegionInfo().getTable().getNameAsString();

            byte[] row = put.getRow();
            Get get = new Get(row);
            Result result = env.getRegion().get(get);
            CellScanner scanner = result.cellScanner();
            int n = 0;
            while (scanner.advance()) {
                Cell cell = scanner.current();
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                byte[] family = CellUtil.cloneFamily(cell);
                n++;
            }

            UUID idOne = UUID.randomUUID();
            byte[] idRow =  Bytes.toBytes(idOne.toString());

            Put newPut = new Put(idRow);
            newPut.addColumn(CF,ACCION,Bytes.toBytes("PUT"));
            newPut.addColumn(CF,ID,row);
            newPut.addColumn(CF,TABLE_NAME,Bytes.toBytes(tablename));
            newPut.addColumn(CF,NCOLUMNS,Bytes.toBytes(Integer.toString(n)));
            audit.put(newPut);

        } catch (Exception e1) {

        }


    }

/*    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put,
                        WALEdit edit,
                        Durability durability)
            throws IOException {

        RegionCoprocessorEnvironment env = e.getEnvironment();
        Table auditTable = null;

        try {

            auditTable = env.getTable(TableName.valueOf(TABLE));
            System.out.println("Ejecutando proceso ReplicaExample");

            String tablename = env.getRegion().getTableDesc().getNameAsString();

            byte[] row = put.getRow();
            Get get = new Get(row);
            Result result = env.getRegion().get(get);
            CellScanner scanner = result.cellScanner();
            int n = 0;
            while (scanner.advance()) {
                Cell cell = scanner.current();
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                byte[] family = CellUtil.cloneFamily(cell);
                n++;
            }

            UUID idOne = UUID.randomUUID();
            byte[] idRow = Bytes.toBytes(idOne.toString());
            Put newPut = new Put(Bytes.toBytes("a"));
            //Put newPut = new Put(idRow);
//            newPut.addColumn(CF,ACCION,Bytes.toBytes("PUT"));
//            newPut.addColumn(CF,ID,row);
//            newPut.addColumn(CF,TABLE_NAME,Bytes.toBytes(tablename));
//            newPut.addColumn(CF,NCOLUMNS,Bytes.toBytes(n));
            auditTable.put(newPut);

        } finally {
            auditTable.close();
        }


    }*/

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
       audit.close();
    }
}
