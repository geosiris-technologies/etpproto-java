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

import Energistics.Etp.v12.Datatypes.Version;
import Energistics.Etp.v12.Etp;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
public class ETPinfo {
	public static final Logger logger = LogManager.getLogger(ETPinfo.class);
	
	public final static Version ETP_VERSION = getETPVersion();
	
	private final static Map<String, Integer> MAP_PROTOCOL_NAME_TO_NUMBER = getProtocolsNumbers();
	
	
	public static Integer getProtocolNumber(String protocolName) {
		if(MAP_PROTOCOL_NAME_TO_NUMBER.containsKey(protocolName)) {
			return MAP_PROTOCOL_NAME_TO_NUMBER.get(protocolName);
		}else {
			for(String prot : MAP_PROTOCOL_NAME_TO_NUMBER.keySet()) {
				if(prot.compareToIgnoreCase(protocolName)==0) {
					return MAP_PROTOCOL_NAME_TO_NUMBER.get(prot);
				}
			}
		}
		return null;
	}
	
	/**
	 * Return a property value by searching it in the AVRO schema of the class. 
	 * To work, the class must have a static method called "getClassSchema" that returns a Schema
	 * @param objClass
	 * @param propName
	 * @return the property value
	 */
	public static String getProp(Class<? extends SpecificRecordBase> objClass, String propName) {
		try {
			Method m = objClass.getMethod("getClassSchema");
//			logger.info(m.invoke(null));
			return ((Schema) m.invoke(null)).getProp(propName);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
				| SecurityException e) { 
			logger.error(e.getMessage()); 
			logger.debug(e.getMessage(), e);
		}
		return null;
	}
	
	/**
	 * Gives an etp protocol name from it's number
	 * @param protocolNum
	 * @return 
	 */
	public static String getProtocolName(Integer protocolNum) {
		for(String s : MAP_PROTOCOL_NAME_TO_NUMBER.keySet()) {
			if(Objects.equals(MAP_PROTOCOL_NAME_TO_NUMBER.get(s), protocolNum)) {
				return s;
			}
		}
		return null;
	}
	
	private static Version getETPVersion() {
		String etpVersion = "" + Etp.PROTOCOL.getObjectProp("version");
		int major = 0;
		int minor = 0;
		int revision = 0;
		int patch = 0;
		
		String[] vSplited = etpVersion.split("\\.");
		
		if(vSplited.length>0){
			major = Integer.parseInt(vSplited[0]);
		}
		if(vSplited.length>1){
			minor = Integer.parseInt(vSplited[1]);
		}
		if(vSplited.length>2){
			revision = Integer.parseInt(vSplited[2]);
		}
		if(vSplited.length>3){
			patch = Integer.parseInt(vSplited[3]);
		}
		return new Version(major, minor, revision, patch);
	}

	
	private static Map<String, Integer> getProtocolsNumbers(){
		Map<String, Integer> protocolMap = new HashMap<>();
		for(Schema sc : Etp.PROTOCOL.getTypes()) {
			if(sc.getNamespace().contains("Protocol.")) {
				String protocolName = sc.getNamespace().substring(sc.getNamespace().lastIndexOf(".") + 1);
				if(!protocolMap.containsKey(protocolName)) {
					protocolMap.put(protocolName, Integer.parseInt(sc.getProp("protocol")));
				}
			}
		}
		return protocolMap;
	}
	
}
