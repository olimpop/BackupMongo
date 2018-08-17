/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package conexion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
  */
public class conexionPostgres {

    private Connection cnx;
    private boolean conect = false;

    public void Conectar() {
        try {
            String driver = "org.postgresql.Driver";
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(conexionPostgres.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Cargando driver: " + driver);            
            String url = "jdbc:postgresql://localhost:5432/BDPostgres";            
            Properties info = new Properties();            
            info.setProperty("user", "user_backups");
            info.setProperty("password", "user_backups");            
            cnx = DriverManager.getConnection(url, info);
            //System.out.println("Iniciando conexión " + url);
        } catch (SQLException e) {
            e.printStackTrace();
            javax.swing.JOptionPane.showMessageDialog(null, "1. La base de datos no se encuentra lista",
                    "Error de Base de Datos", javax.swing.JOptionPane.INFORMATION_MESSAGE);
            System.err.println("-----------SQLException------------\n");
            System.err.println("Mensaje:" + e.getMessage());
            System.err.println("Estado SQL:" + e.getSQLState());
            System.err.println("Codigo de Error:" + e.getErrorCode());
        }
        System.out.println("Conectando.........." + cnx + "........");
        System.out.println();
    }

    public void CloseConection() {
        try {
            cnx.close();
            conect = false;
            System.out.println();
            System.out.println("Finalizada la conexión");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isConectado() {
        return conect;
    }

    public Connection getConexion() {
        return this.cnx;
    }
}
