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
package com.geosiris.etp.utils;

import Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayIdentifier;
import Energistics.Etp.v12.Datatypes.Object.*;
import Energistics.Etp.v12.Datatypes.SupportedDataObject;
import Energistics.Etp.v12.Datatypes.Uuid;
import Energistics.Etp.v12.Protocol.Core.CloseSession;
import Energistics.Etp.v12.Protocol.Core.RequestSession;
import Energistics.Etp.v12.Protocol.DataArray.GetDataArrayMetadata;
import Energistics.Etp.v12.Protocol.DataArray.GetDataArrays;
import Energistics.Etp.v12.Protocol.Discovery.GetResources;
import Energistics.Etp.v12.Protocol.Store.DeleteDataObjects;
import Energistics.Etp.v12.Protocol.Store.GetDataObjects;
import Energistics.Etp.v12.Protocol.Store.PutDataObjects;
import com.geosiris.etp.websocket.ETPClientSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class ETPDefaultProtocolBuilder {
	public static final Logger logger = LogManager.getLogger(ETPDefaultProtocolBuilder.class);

	public static RequestSession buildRequestSession(Uuid uuid) {
		List<SupportedDataObject> supportedDataObjects = new ArrayList<>();


//		Map<CharSequence, DataValue> supportedList = new HashMap<>();
//		supportedList.put("resqml20", DataValue.newBuilder().setItem(null).build());
//		SupportedDataObject sdo = SupportedDataObject.newBuilder()
//				.setDataObjectCapabilities(supportedList)
//				.setQualifiedType("resqml20")
//				.build();
//		supportedDataObjects.add(sdo);

		List<CharSequence> supportedCompression = new ArrayList<>();
//		supportedCompression.add("string");

		Date d = new Date();
		return RequestSession.newBuilder()
				.setApplicationName("WebStudio")
				.setApplicationVersion("1.2")
				.setClientInstanceId(uuid)
				.setCurrentDateTime(d.getTime())
				.setEarliestRetainedChangeTime(0)
				.setRequestedProtocols(ETPClientSession.SUPPORTED_PROTOCOLS)
				.setSupportedDataObjects(supportedDataObjects)
				.setSupportedCompression(supportedCompression)
				.setSupportedFormats(new ArrayList<>())
				.build();
	}

	public static CloseSession buildCloseSession(String reason) {
		return CloseSession.newBuilder()
				.setReason(reason)
				.build();
	}


//	public static GetResources buildGetResources(String uri, ContextScopeKind scope, List<CharSequence> objectTypes) {
//		if(uri == null) {
//			uri = "eml:///";
//		}
//		GetResources getRess = 
//				GetResources.newBuilder()
//				.setContext(ContextInfo.newBuilder()
//						.setDataObjectTypes(objectTypes)
//						.setDepth(1)
//						.setUri(uri)
//						.setRelationshipKind(RelationshipKind.Both)
//						.build())
//				.setScope(scope)
//				.setStoreLastWriteFilter(0L)
//				.setActiveStatusFilter(null)
//				.setCountObjects(false)
//				.setIncludeEdges(false)
//				.build();
//		return getRess;
//	}
	
	public static GetResources buildGetResources(String uri, ContextScopeKind scope, List<CharSequence> objectTypes) {
		return buildGetResources(uri, scope, objectTypes, 1);
	}
	
	public static GetResources buildGetResources(String uri, 
												ContextScopeKind scope, 
												List<CharSequence> objectTypes, 
												int depth) {
		if(uri == null) {
			uri = "eml:///";
		}
		//		logger.error(getRess);
		return GetResources.newBuilder()
		.setContext(ContextInfo.newBuilder()
				.setDataObjectTypes(objectTypes)
				.setDepth(depth)
				.setUri(uri)
				.setNavigableEdges(RelationshipKind.Primary)
				.setIncludeSecondarySources(false)
				.setIncludeSecondaryTargets(false)
				.build())
		.setScope(scope)
		.setStoreLastWriteFilter(null)
		.setActiveStatusFilter(null)
		.setCountObjects(true)
		.setIncludeEdges(false)
		.build();
	}

	public static DeleteDataObjects buildDeleteDataObjects(Map<CharSequence, CharSequence> uris) {
		return DeleteDataObjects.newBuilder()
				.setUris(uris)
				.build();
	}

	public static GetDataObjects buildGetDataObjects(Map<CharSequence, CharSequence> uris, String format) {
		return GetDataObjects.newBuilder()
				.setUris(uris)
				.setFormat(format)
				.build();
	}

	public static PutDataObjects buildPutDataObjects(Map<CharSequence, DataObject> map, boolean prune) {
		return PutDataObjects.newBuilder()
				.setDataObjects(map)
				.setPruneContainedObjects(prune)
				.build();
	}

	public static DataObject buildDataObjectFromResqmlXMl(String xml, String uuid, Resource resource) {
		ByteBuffer bf  = ByteBuffer.wrap(xml.getBytes(CHARSET));
		return DataObject.newBuilder()
				.setResource(resource)
				.setFormat("xml")
				.setData(bf)
				.setBlobId(new Uuid(uuid.getBytes()))
				.build();
	}

	private final static Pattern REGEXP_UUID_IN_URI = Pattern.compile("\\(([\\da-zA-Z\\-]+)\\)");
	
	private final static Pattern REGEXP_UUID = Pattern.compile("[Uu][Uu][Ii][Dd]\\s*=\\s*\"[^\"]+\"");
	private final static Charset CHARSET = StandardCharsets.UTF_8;

	public static PutDataObjects buildPutDataObjects(List<Pair<String, Resource>> resqmlXmlData, 
			boolean prune) {
		Map<CharSequence, DataObject> map = new HashMap<>();
		for(Pair<String, Resource> xmlNres : resqmlXmlData) {
			Matcher match = REGEXP_UUID_IN_URI.matcher(xmlNres.r().getUri());
			if(match.find()) {
				String uuid = match.group(1);
				DataObject data = buildDataObjectFromResqmlXMl(xmlNres.l(), uuid, xmlNres.r());
				map.put(xmlNres.r().getUri()+"", data);
			}
		}

		return buildPutDataObjects(map, prune);
	}

	public static PutDataObjects buildPutDataObjects(String xmlContent, Resource resource, boolean prune) {
		List<Pair<String, Resource>> resList = new ArrayList<>();
		resList.add(new Pair<>(xmlContent, resource));
		return buildPutDataObjects(resList, prune);
	}
	
//	public static void main(String[] argv) {
//		Matcher match = regexpUUID_inURI.matcher("eml:///resqml20.obj_GeneticBoundaryFeature(a58b52f3-9723-41e3-a2be-98e395a7aa11)");
//		if(match.find()) {
//			logger.info(match.group(1));
//		}
//		logger.info("FIN");
//	}
	
//	public static GetDataArrays buildGetDataArrays() {
//		return GetDataArrays.newBuilder().setDataArrays(value)
//	}
	
	public static GetDataArrayMetadata buildGetDataArrayMetadata(Map<CharSequence, DataArrayIdentifier> map) {
		return GetDataArrayMetadata.newBuilder().setDataArrays(map).build();
	}
	

	public static GetDataArrayMetadata buildGetDataArrayMetadata(List<DataArrayIdentifier> identifierList) {
		Map<CharSequence, DataArrayIdentifier> map = new HashMap<>();
		for(DataArrayIdentifier dai: identifierList) {
			map.put("req_dai_" + map.size(), dai);
		}
		return buildGetDataArrayMetadata(map);
	}

	public static GetDataArrays buildGetDataArrays(List<Pair<String, String>> uriAndPathInHDF){
		Map<CharSequence, DataArrayIdentifier> map = new HashMap<>();

		for(Pair<String, String> p : uriAndPathInHDF) {
			map.put("" + map.size(), DataArrayIdentifier.newBuilder()
					.setUri(p.l())
					.setPathInResource(p.r())
					.build());
		}
		return GetDataArrays.newBuilder().setDataArrays(map).build();
	}
}
