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
package com.geosiris.etp.communication;

import Energistics.Etp.v12.Datatypes.MessageHeader;
import Energistics.Etp.v12.Etp;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
public class ProtocolsUtility {
	public static final Logger logger = LogManager.getLogger(ProtocolsUtility.class);
	public static final Map<Integer, Map<Integer, Schema>> etpProtocolTypes = initProtocols();

	public static <T extends SpecificRecordBase> void decode(T obj, Decoder dec) throws IOException {
		SpecificDatumReader<T> reader = new SpecificDatumReader<>(obj.getSchema(), obj.getSchema());
		reader.read(obj, dec);
	}

	public static Object handleMessage(MessageHeader mh, Decoder dec) throws NoSuchMethodException, IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
		Class<?> etpObjClass = getEtpClassFromIds(mh.getProtocol(), mh.getMessageType());
		// logger.info("Trying to find class for protocol '" + mh.getProtocol() +"' messageType '" + mh.getMessageType() +"' -> " + etpObjClass);
		Object etpObject;
		assert etpObjClass != null;
		etpObject = etpObjClass.getConstructor().newInstance();
		decode((SpecificRecordBase)etpObject, dec);
		logger.info("Received objet [" + etpObject.getClass() + "]\n" + etpObject);

		return etpObject;
	}

	public static Object handleMessageJSON(MessageHeader mh, String msg){
		Class<?> etpObjClass = getEtpClassFromIds(mh.getProtocol(), mh.getMessageType());
		// logger.info("Trying to find class for protocol '" + mh.getProtocol() +"' messageType '" + mh.getMessageType() +"' -> " + etpObjClass);
		SpecificRecordBase etpObject = null;
		try {
			logger.info("Try to read json from : " + msg);
			assert etpObjClass != null;
			etpObject = (SpecificRecordBase) etpObjClass.getConstructor().newInstance();
			JsonDecoder jsonDec = DecoderFactory.get().jsonDecoder(etpObject.getSchema(), msg);
			decode(etpObject, jsonDec);
			logger.info("Recived objet [" + etpObject.getClass() + "]\n" + etpObject);
		} catch (InstantiationException | IllegalAccessException | IOException | NoSuchMethodException |
				 InvocationTargetException e) {  logger.error(e.getMessage());  logger.debug(e.getMessage(), e); }
		return etpObject;
	}

	public static Class<?> getEtpClassFromIds(int protocolId, int messageType){
		if(etpProtocolTypes.containsKey(protocolId)
				&& etpProtocolTypes.get(protocolId).containsKey(messageType)) {
			try {
				return Class.forName(etpProtocolTypes.get(protocolId).get(messageType).getFullName());
			} catch (ClassNotFoundException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
		}
		return null;
	}

	public static Class<?> getEtpClassFromProtocolIdAndName(int protocolId, String className){
		if(etpProtocolTypes.containsKey(protocolId)) {
			for(Schema scheme : etpProtocolTypes.get(protocolId).values()){
				if(scheme.getName().compareToIgnoreCase(className) == 0){
					try {
						return Class.forName(scheme.getFullName());
					} catch (ClassNotFoundException e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
				}
			}
		}
		return null;
	}


	private static Map<Integer, Map<Integer, Schema>> initProtocols() {
		Map<Integer, Map<Integer, Schema>> result = new HashMap<>();

		for(Schema sc : Etp.PROTOCOL.getTypes()) {
			//logger.info("====> " + sc.getFullName());
			try {
				//logger.info(sc.getProp("protocol"));
				Integer protocolId = Integer.parseInt(sc.getProp("protocol"));
				if(!result.containsKey(protocolId)) {
					result.put(protocolId, new HashMap<>());
				}
				Integer messageType = Integer.parseInt(sc.getProp("messageType"));
				if(!result.get(protocolId).containsKey(messageType)) {
					result.get(protocolId).put(messageType, sc);
				}else {
					logger.error("Conflict for message type '" + messageType +"' for protocol '" + protocolId + "'");
				}

			}catch (Exception e) {
				//e.printStackTrace();
				//logger.error("Error for type : " + sc.getName());
				//				for(String p : sc.getObjectProps().keySet()) {
				//					logger.info("\t" + p + " : " + sc.getObjectProps().get(p));
				//				}
			}
		}

		return result;
	}


	public static void main(String[] argv) {
		Map<Integer, Map<Integer, Schema>> result = initProtocols();

		for(Integer k : result.keySet()) {
			Map<Integer, Schema> p = result.get(k);
			logger.info("{ \"" + k + "\" :");
			for(Integer k2 : p.keySet()) {
				Schema p2 = p.get(k2);
				logger.info("\t{ \"" + k2 + "\" : " + p2.getName() +"},\n");
			}
			logger.info("},\n");
		}
	}

}
