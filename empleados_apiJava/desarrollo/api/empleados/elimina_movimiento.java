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


public class elimina_movimiento extends MbJavaComputeNode {
	public String url = "jdbc:mysql://localhost:3306/desarrolloApp?allowPublicKeyRetrieval=true";

	public void evaluate(MbMessageAssembly inAssembly) throws MbException {
		MbOutputTerminal out = getOutputTerminal("out");
		//MbOutputTerminal alt = getOutputTerminal("alternate");

		MbMessage inMessage = inAssembly.getMessage();
		MbMessageAssembly outAssembly = null;
		try {
			
			MbMessage outMessage = new MbMessage();
			outAssembly = new MbMessageAssembly(inAssembly, outMessage);
		
			MbElement outRoot = outMessage.getRootElement();

			MbElement outHTTPRequest = outRoot.createElementAsLastChild("HTTPRequestHeader");
			outHTTPRequest.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json; charset=utf-8"); //charset=utf-8 || charset=ISO-8859-1

			MbElement outJsonRoot = outRoot.createElementAsLastChild(MbJSON.PARSER_NAME);
			MbElement outJsonData = outJsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null);

			//Vaciar el JSON de entrada en un properties
			Properties props = new Properties();
			MbElement entrada = inMessage.getRootElement().getLastChild().getFirstElementByPath("/JSON/Data").getFirstChild();
						
			while(entrada != null ) {			
					props.setProperty(entrada.getName(), entrada.getValueAsString());
					entrada = entrada.getNextSibling();
					
			} 
			
			
			CallableStatement stmBaja=null;
			Connection conn = null;			
			
			try
			   {
			     Class.forName("com.mysql.jdbc.Driver");
			     conn = DriverManager.getConnection(url,"root","usrPassw0rd");
			     //consultar el ultimo empleado registrado o inicial
			     stmBaja = conn.prepareCall("{call sp_eliminar_movimiento(?, ?, ?, ?)}");
			     stmBaja.setInt(1,Integer.parseInt(props.getProperty("Movimiento")));
			     stmBaja.setInt(2,Integer.parseInt(props.getProperty("Empleado")));
			     stmBaja.registerOutParameter("codigoRespuesta", Types.CHAR);//Tipo String
			     stmBaja.registerOutParameter("mensaje", Types.VARCHAR);//Tipo String            
			     
			     stmBaja.execute();
	             
	             if(!stmBaja.equals(null)){
	            	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"codigoRespuesta",stmBaja.getString(3));
			     	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"mensaje",stmBaja.getString(4));
	             }else{
			    	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"codigoRespuesta","11111");
			     	 outJsonData.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"mensaje","ha ocurrido un error");
	             }
	               
			   }catch(Exception e){
			    throw e;
			   }finally{
				 stmBaja.close();
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
