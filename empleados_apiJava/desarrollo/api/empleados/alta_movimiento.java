package desarrollo.api.empleados;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.Properties;

import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;


public class alta_movimiento extends MbJavaComputeNode {
	public String url = "jdbc:mysql://localhost:3306/desarrolloApp?allowPublicKeyRetrieval=true";

	public void evaluate(MbMessageAssembly inAssembly) throws MbException {
		MbOutputTerminal out = getOutputTerminal("out");
		//MbOutputTerminal alt = getOutputTerminal("alternate");

		MbMessage inMessage = inAssembly.getMessage();
		MbMessageAssembly outAssembly = null;
		try {
			
			MbMessage outMessage = new MbMessage();
			outAssembly = new MbMessageAssembly(inAssembly, outMessage);
			// ----------------------------------------------------------
			// Add user code below
			// Crear estructura de mensaje JSON/Data 
			MbElement outRoot = outMessage.getRootElement();

			MbElement outHTTPRequest = outRoot.createElementAsLastChild("HTTPRequestHeader");
			outHTTPRequest.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json; charset=utf-8"); //charset=utf-8 || charset=ISO-8859-1

			MbElement outJsonRoot = outRoot.createElementAsLastChild(MbJSON.PARSER_NAME);
			MbElement outJsonData = outJsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null);

			//Vaciar el JSON de entrada en un properties
			Properties props = new Properties();
			MbElement entrada = inMessage.getRootElement().getLastChild().getFirstElementByPath("/JSON/Data").getFirstChild();
						
			while(entrada != null ) {			
				if(!entrada.getName().equals("BusquedaNombre")){
					props.setProperty(entrada.getName(), entrada.getValueAsString());
					entrada = entrada.getNextSibling();
					
				}else{
					entrada = entrada.getNextSibling();
				}
			} 
			
			
			CallableStatement cStmt=null;
			Connection conn = null;
						
			try
			   {
			     Class.forName("com.mysql.jdbc.Driver");
			     conn = DriverManager.getConnection(url,"root","usrPassw0rd");
			       
	             cStmt = conn.prepareCall("{call sp_registra_movimientos(?, ?, ?, ?, ?, ?, ?)}");
	             cStmt.setInt(1, Integer.parseInt(props.getProperty("IdEmpleado")));
			   	 cStmt.setString(2,props.getProperty("NombreEmpleado"));    
	             cStmt.setInt(3, Integer.parseInt(props.getProperty("EntregasDiaria")));  
	             cStmt.setString(4, props.getProperty("FechaCaptura"));  
	             cStmt.setInt(5, Integer.parseInt(props.getProperty("Turno")));  
	             cStmt.registerOutParameter("codigoRespuesta", Types.CHAR);//Tipo String
	             cStmt.registerOutParameter("mensaje", Types.VARCHAR);//Tipo String            
	             cStmt.execute();
	             
	             if(!cStmt.equals(null)){
	            	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"codigoRespuesta",cStmt.getString(6));
			     	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"mensaje",cStmt.getString(7));
	             }else{
			    	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"codigoRespuesta","11111");
			     	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"mensaje","ha ocurrido un error");
	             }
	               
			   }catch(Exception e){
			    throw e;
			   }finally{
			     cStmt.close();
			     conn.close();
			   }
			// End of user code
			// ----------------------------------------------------------
		} catch (MbException e) {
			// Re-throw to allow Broker handling of MbException
			throw e;
		} catch (RuntimeException e) {
			// Re-throw to allow Broker handling of RuntimeException
			throw e;
		} catch (Exception e) {
			// Consider replacing Exception with type(s) thrown by user code
			// Example handling ensures all exceptions are re-thrown to be handled in the flow
			throw new MbUserException(this, "evaluate()", "", "", e.toString(),
					null);
		}
		// The following should only be changed
		// if not propagating message to the 'out' terminal
		out.propagate(outAssembly);

	}
}
