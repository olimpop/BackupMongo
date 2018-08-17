/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package backupmongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import conexion.*;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 
 */
public class BackupMongoD {

    /**
     * @param args the command line arguments
     */
    private static Mongo mongoBkp;
    private static DB dbBkp;
    private static DBCollection collectionBkp;
    private static GridFS gfsPhotoBkp;
    //
    private static Mongo mongoPk;
    private static DB dbPk;
    private static DBCollection collectionPk;
    private static GridFS gfsPhotoPk;

    public static void main(String[] args) {
        // TODO code application logic here
        conexionPostgres cnx = new conexionPostgres();
        String FileName = "";
        try {
            ResultSet rs;
            cnx.Conectar();
            Statement stmt = cnx.getConexion().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            String query = "select id_documento "
                    + "from documento_d "
                    + "where adjunta_documento is true "
                    + "order by 1";
            rs = stmt.executeQuery(query);
            rs.last();
            System.out.println("Total Postgres " + rs.getRow());
            rs.beforeFirst();
            conexionMongo();
            int flag = 1;
            while (rs.next()) {
                FileName = "" + rs.getInt("id_documento");
                GridFSDBFile iOBkp = gfsPhotoBkp.findOne(FileName);
                
                GridFSDBFile iOPk = gfsPhotoPk.findOne(FileName);
                try {
                    iOBkp.validate();
                } catch (Exception e) {
                    iOBkp = null;
                }
                try {
                    iOPk.validate();
                } catch (Exception e) {
                    //gfsPhotoPk.remove(FileName);
                    iOPk = null;
                }
                System.out.println("D: " + flag++ + " - " + FileName);                
                if ((iOPk != null && iOBkp == null)) {
                    GridFSInputFile gfsFileBkp = gfsPhotoBkp.createFile(iOPk.getInputStream());
                    gfsFileBkp.setFilename(iOPk.getFilename());
                    gfsFileBkp.save();
                    iOBkp = gfsPhotoBkp.findOne(FileName);
                    System.out.println("id_documento_d: " + FileName);
                    System.out.println("documentoBkp: " + iOBkp);
                    System.out.println("documentoPk: " + iOPk);
                }
            }
        } catch (Exception e) {
            System.out.println("D " + FileName);
            e.printStackTrace();
            System.out.println(e.getCause());
            System.out.println(e.getMessage());
        } finally {
            cnx.CloseConection();
        }
    }

    private static void conexionMongo() {
        try {
            mongoBkp = new Mongo("localhost", 27017);
            dbBkp = mongoBkp.getDB("db");
            collectionBkp = dbBkp.getCollection("documento_d_anexo");
            gfsPhotoBkp = new GridFS(dbBkp, "documento_d_anexo");
            //
            mongoPk = new Mongo("remoto", 27017);
            dbPk = mongoPk.getDB("db");
            collectionPk = dbPk.getCollection("documento_d_anexo");
            gfsPhotoPk = new GridFS(dbPk, "documento_d_anexo");
        } catch (UnknownHostException ex) {
            System.out.println("D ");
            Logger.getLogger(BackupMongoD.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        } catch (MongoException ex) {
            System.out.println("D ");
            Logger.getLogger(BackupMongoD.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }
    }

}
