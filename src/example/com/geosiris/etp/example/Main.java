/*
Copyright 2019 GEOSIRIS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package com.geosiris.etp.example;

import Energistics.Etp.v12.Datatypes.ServerCapabilities;
import com.geosiris.etp.communication.ClientInfo;
import com.geosiris.etp.communication.ConnectionType;
import com.geosiris.etp.communication.ETPConnection;
import com.geosiris.etp.protocols.CommunicationProtocol;
import com.geosiris.etp.protocols.ProtocolHandler;
import com.geosiris.etp.protocols.handlers.generated.*;
import com.geosiris.etp.utils.ETPHelper;
import com.geosiris.etp.websocket.ETPClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.HttpURI;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class Main {
	public static Logger logger = LogManager.getLogger(Main.class);
	public static String file1 = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
			+"<resqml22:BoundaryFeature xmlns:resqml22=\"http://www.energistics.org/energyml/data/resqmlv2\" xmlns:eml23=\"http://www.energistics.org/energyml/data/commonv2\" uuid=\"5fa99eb4-b11f-4f08-b1be-2d64ff14286f\" schemaVersion=\"2.2\">"
			+"    <eml23:Citation>"
			+"        <eml23:Title>Hugin_Fm_Top</eml23:Title>"
			+"        <eml23:Originator>ATsoblefack</eml23:Originator>"
			+"        <eml23:Creation>2018-11-23T15:59:25Z</eml23:Creation>"
			+"        <eml23:Format>Paradigm SKUA-GOCAD 19 Alpha 2 Build://skua-gocad/Production/trunk - 20190322-cl867561 for Win_x64_6.1_v15</eml23:Format>"
			+"    </eml23:Citation>"
			+"    <resqml22:IsWellKnown>false</resqml22:IsWellKnown>"
			+"</resqml22:BoundaryFeature>";

	public static void main(String[] args) throws Exception {
		etpClientTest(args);
	}

	public static void etpClientTest(String args[]){
		logger.info("Usage : java -jar myfile.jar [SERVER_URL] [LOGIN] [PASSWORD]");

		HttpURI etpServerUri = null;
		String login = "";
		String password = "";

		if (args.length > 0) {
			try { etpServerUri = new HttpURI(args[0]);
			}catch(Exception e) {e.printStackTrace();}
		}
		if (args.length > 1) {
			try { login = args[1];
			}catch(Exception e) {e.printStackTrace();}
		}
		if (args.length > 2) {
			try { password = args[2];
			}catch(Exception e) {e.printStackTrace();}
		}
		logger.info(etpServerUri);

		ClientInfo clientInfo = new ClientInfo(etpServerUri, 32768, 65536);
		Map<CommunicationProtocol, ProtocolHandler> protocolHandlers = new HashMap<>();
		protocolHandlers.put(CoreHandler_DefaultPrinter.protocol, new CoreHandler_DefaultPrinter());
		protocolHandlers.put(StoreHandler_DefaultPrinter.protocol, new StoreHandler_DefaultPrinter());
		protocolHandlers.put(DataspaceHandler_DefaultPrinter.protocol, new DataspaceHandler_DefaultPrinter());
		protocolHandlers.put(DataArrayHandler_DefaultPrinter.protocol, new DataArrayHandler_DefaultPrinter());
		protocolHandlers.put(DiscoveryHandler_DefaultPrinter.protocol, new DiscoveryHandler_DefaultPrinter());

		ETPConnection etpConnection = new ETPConnection(ConnectionType.CLIENT, new ServerCapabilities(), clientInfo, protocolHandlers);

		ETPClient etpClient = null;
		if(true||args.length >= 3) {
			etpClient = ETPClient.getInstanceWithAuth_Basic(etpServerUri, etpConnection, 2000, login, password);
		}else if(args.length >= 3) {
			etpClient = ETPClient.getInstanceWithAuth_Token(etpServerUri, etpConnection, 10000, args[1]);
		}



		if(etpClient != null){
			{
				List<Number> triangles = launchGetDataArray(etpClient, "eml:///dataspace('usecase1-2')/resqml22.TriangulatedSetRepresentation(07f6ac13-966d-427f-b780-bdaf26b494dd)",
											"/resqml22/07f6ac13-966d-427f-b780-bdaf26b494dd/triangles_patch0",
												true, false, false);

				List<Number> points = launchGetDataArray(etpClient, "eml:///dataspace('usecase1-2')/resqml22.TriangulatedSetRepresentation(07f6ac13-966d-427f-b780-bdaf26b494dd)",
											"/resqml22/07f6ac13-966d-427f-b780-bdaf26b494dd/points_patch0",
												true, false, false);

				for(Number n : triangles){
					//System.out.print("c_" + points.get(n.intValue()*3) + " ");
					logger.info("(" + points.get(n.intValue()*3) + "; "+ points.get(n.intValue()*3 + 1) + "; "+ points.get(n.intValue()*3 + 2) + ")");
				}
				logger.info("PointSize : " + points.size());
			}

			etpClient.closeClient();
		}
	}

	public static List<Number> launchGetDataArray(ETPClient etpClient, String uri, String path, boolean useSubArray, boolean printTable, boolean compareWithoutSubArray){
		List<Number> allPoints = ETPHelper.sendGetDataArray_prettier(etpClient, uri, path, 50000, useSubArray);

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(allPoints);
			logger.info(">> Sizeof array : " + bos.toByteArray().length + " bytes");
		}catch (Exception e){
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}

		if(printTable) {
			logger.info("allPoints " + allPoints.size());
			for (Number n : allPoints) {
				System.out.print(n + " ");
			}
			logger.info("");
		}

		if(useSubArray && compareWithoutSubArray){
			List<Number> allPoints_2 = ETPHelper.sendGetDataArray_prettier(etpClient, uri, path, 50000, false);
			if(printTable) {
				logger.info("allPoints_2 " + allPoints_2.size());
				for (Number n : allPoints_2) {
					System.out.print(n + " ");
				}
				logger.info("");
			}
			assert allPoints_2.size() == allPoints.size();
			for(int i=0; i<allPoints_2.size(); i++){
				assert allPoints.get(i) == allPoints_2.get(i);
			}
		}
		return allPoints;
	}
}
