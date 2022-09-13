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

import Energistics.Etp.v12.Datatypes.AnyArray;
import Energistics.Etp.v12.Datatypes.AnyLogicalArrayType;
import Energistics.Etp.v12.Datatypes.ArrayOfFloat;
import Energistics.Etp.v12.Datatypes.DataArrayTypes.*;
import Energistics.Etp.v12.Datatypes.Object.*;
import Energistics.Etp.v12.Protocol.DataArray.*;
import Energistics.Etp.v12.Protocol.Discovery.GetResources;
import Energistics.Etp.v12.Protocol.Discovery.GetResourcesResponse;
import Energistics.Etp.v12.Protocol.Store.GetDataObjects;
import Energistics.Etp.v12.Protocol.Store.GetDataObjectsResponse;
import Energistics.Etp.v12.Protocol.Store.PutDataObjects;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.websocket.ETPClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.*;
public class ETPHelper {
	public static final Logger logger = LogManager.getLogger(ETPHelper.class);
	/* PuDataObjects */

	public static List<Message> sendPutDataObjects_from_file(ETPClient etpClient, List<String> filePaths, String dataspace, int timeoutMS){
		if(filePaths.size()>0) {
			List<Pair<String, Resource>> ressource_list = new ArrayList<>();
			List<String> filesContent = new ArrayList<>();
			for (String filePath : filePaths) {
				StringBuilder fileContent = new StringBuilder();
				try {
					File myObj = new File(filePath);
					Scanner myReader = new Scanner(myObj);
					while (myReader.hasNextLine()) {
						String data = myReader.nextLine();
						fileContent.append(data);
					}
					myReader.close();
					filesContent.add(fileContent.toString());
				} catch (FileNotFoundException e1) {
					logger.error(e1.getMessage());
					logger.debug(e1.getMessage(), e1);
				}
			}
			return sendPutDataObjects(etpClient, filesContent, dataspace, timeoutMS);
		}
		return null;
	}
	public static List<Message> sendPutDataObjects(ETPClient etpClient, List<String> filesContent, String dataspace, int timeoutMS){
		if(filesContent.size()>0) {
			List<Pair<String, Resource>> ressource_list = new ArrayList<>();

			for (String fcontent : filesContent) {
				Document xmlDoc = null;
				try {
					xmlDoc = XmlUtils.readXml(fcontent);
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
				Date d = new Date();
				assert xmlDoc != null;
				Resource fResource = Resource.newBuilder()
						.setAlternateUris(new ArrayList<>())
						.setCustomData(new HashMap<>())
						.setLastChanged(d.getTime())
						.setName(XmlUtils.xml_getSubNodecontent(xmlDoc, "Citation.Title"))
						.setSourceCount(null)
						.setTargetCount(null)
						.setStoreCreated(0)
						.setActiveStatus(ActiveStatusKind.Active)
						.setStoreLastWrite(d.getTime())
						.setUri(XmlUtils.getUriInXml(xmlDoc, dataspace))
						.build();

				ressource_list.add(new Pair<>(fcontent, fResource));
			}

			PutDataObjects pdo = ETPDefaultProtocolBuilder.buildPutDataObjects(ressource_list, false);
			long pdi_id = etpClient.send(pdo);
//			logger.info(pdi_id + ") Waiting for Put data answer");
			return etpClient.getEtpClientSession().waitForResponse(pdi_id, timeoutMS);
//			logger.info(pdi_id + ") Answer : " + dataResp);
		}
		return null;
	}

	/* GetDataObjects */

	/**
	 * Send GetDatObject request, to get the xml content of an object, identified by its uri.
	 * @param etpClient
	 * @param objUri the object uri
	 * @return server messages answers
	 */
	public static List<Message> sendGetDataObjects(ETPClient etpClient, String objUri, String format, int timeoutMS){
		List<String> ll = new ArrayList<>();
		ll.add(objUri);
		return sendGetDataObjects(etpClient, ll, format, timeoutMS);
	}

	/**
	 * Send GetDatObject request, to get the xml content of an objects, identified by their uris.
	 * @param etpClient
	 * @param objectsUris objects uris
	 * @return server messages answers
	 */
	public static List<Message> sendGetDataObjects(ETPClient etpClient, List<String> objectsUris, String format, int timeoutMS){
		Map<CharSequence, CharSequence> mapObj = new HashMap<>();
		for(String seq : objectsUris){
			mapObj.put(mapObj.size()+"", seq);
		}

		GetDataObjects getDataO = ETPDefaultProtocolBuilder.buildGetDataObjects(mapObj, format);
		long getData_id = etpClient.send(getDataO);

//		logger.info(getData_id+") Waiting for GetDataObjects answer\n" + getDataO);
		return etpClient.getEtpClientSession().waitForResponse(getData_id, timeoutMS);
//		logger.info(getData_id+") Answer : " + dataResp);
	}

	/**
	 * Send GetDatObject request, to get the xml content of an objects, identified by their uris.
	 * @param etpClient
	 * @param objectsUris objects uris
	 * @return objects content (xml)
	 */
	public static List<String> sendGetDataObjects_pretty(ETPClient etpClient, List<String> objectsUris, String format, int timeoutMS){
		List<String> result = new ArrayList<>();
		List<Message> msgs = sendGetDataObjects(etpClient, objectsUris, format, timeoutMS);
		for(Message msg : msgs) {
			if (msg.getBody() instanceof GetDataObjectsResponse) {
				GetDataObjectsResponse gdor = (GetDataObjectsResponse) msg.getBody();
				for(DataObject dataObj : gdor.getDataObjects().values()){
					result.add(StandardCharsets.UTF_8.decode(dataObj.getData()).toString());
				}
			}
		}
		return result;
	}
	/**
	 * Send GetDatObject request, to get the xml content of an objects, identified by their uris.
	 * @param etpClient
	 * @param objUri objects uris
	 * @return the object content (xml)
	 */
	public static String sendGetDataObjects_pretty(ETPClient etpClient, String objUri, String format, int timeoutMS){
		List<String> ll = new ArrayList<>();
		ll.add(objUri);
		return sendGetDataObjects_pretty(etpClient, ll, format, timeoutMS).get(0);
	}

	/* GetResources */


	/**
	 * Send a GetResources request to the etp server, with "eml:///" as default uri.
	 * @param etpClient
	 * @param depth depth of search (1 for only current, 2 for direct relations, more for more)
	 * @param scope type of search (incomming/outcomming/both relations)
	 * @return all received messages.
	 */
	public static List<Message> sendGetRessources(ETPClient etpClient, int depth, ContextScopeKind scope, int timeoutMS){
		return sendGetRessources(etpClient, "eml:///", depth, scope, timeoutMS);
	}

	/**
	 * Send a GetResources request to the etp server
	 * @param etpClient
	 * @param uri the object uri
	 * @param depth depth of search (1 for only current, 2 for direct relations, more for more)
	 * @param scope type of search (incomming/outcomming/both relations)
	 * @return all received messages.
	 */
	public static List<Message> sendGetRessources(ETPClient etpClient, String uri, int depth, ContextScopeKind scope, int timeoutMS){
		List<CharSequence> objectTypes = new ArrayList<>();
		GetResources getRess = GetResources.newBuilder()
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
		long getRess_id = etpClient.send(getRess);
//		logger.info(getRess_id+") Waiting for GetResources answer");
		//		logger.info(getRess_id+") Answer : " + ressResp_l);

		return etpClient.getEtpClientSession().waitForResponse(getRess_id, timeoutMS);
	}

	/**
	 * Returns directly a list of URIs after receiving the answer of a GetResources
	 * @param etpClient
	 * @param uri
	 * @param depth depth of search (1 for only current, 2 for direct relations, more for more)
	 * @param scope type of search (incomming/outcomming/both relations)
	 * @return a list of uris as String
	 */
	public static List<String> sendGetRessources_pretty(ETPClient etpClient, String uri, int depth, ContextScopeKind scope, int timeoutMS){
		List<String> result = new ArrayList<>();
		List<Message> msgs = sendGetRessources(etpClient, uri, depth, scope, timeoutMS);
		for(Message msg : msgs) {
			if (msg.getBody() instanceof GetResourcesResponse) {
				GetResourcesResponse objResp = (GetResourcesResponse) msg.getBody();
//				logger.info("Found objects uri : ");
//				logger.info("Nb ressources found : " + objResp.getResources().size());
				for (Resource res : objResp.getResources()) {
//						logger.info(res.getUri());
					result.add("" + res.getUri());
				}
			}
		}
		return result;
	}
	/**
	 * Returns directly a list of URIs after receiving the answer of a GetResources with "eml:///" as default search uri
	 * @param etpClient
	 * @param depth depth of search (1 for only current, 2 for direct relations, more for more)
	 * @param scope type of search (incomming/outcomming/both relations)
	 * @return a list of uris as String
	 */
	public static List<String> sendGetRessources_pretty(ETPClient etpClient, int depth, ContextScopeKind scope, int timeoutMS){
		return sendGetRessources_pretty(etpClient, "eml:///", depth, scope, timeoutMS);
	}

	/* GetDataArray */

	public static List<Message> sendGetDataArray_withSubArray(ETPClient etpClient, String uri, String pathInHDF5, int timeoutMS){

		List<Message> result = new ArrayList<>();

		DataArrayIdentifier identifier = DataArrayIdentifier.newBuilder()
				.setUri(uri)
				.setPathInResource(pathInHDF5)
				.build();

		Map<CharSequence, DataArrayIdentifier> mapIdentifier = new HashMap<>();
		mapIdentifier.put("0", identifier);

		GetDataArrayMetadata meta = GetDataArrayMetadata.newBuilder()
				.setDataArrays(mapIdentifier)
				.build();
		long meta_msg_id = etpClient.send(meta);
		List<Message> meta_resp = etpClient.getEtpClientSession().waitForResponse(meta_msg_id, timeoutMS);
		try {
			for (Message meta_i : meta_resp) {
				GetDataArrayMetadataResponse meta_response = (GetDataArrayMetadataResponse) meta_i.getBody();
				logger.info("META>" + meta_response);

				DataArrayMetadata dam = meta_response.getArrayMetadata().values().iterator().next();
				List<Long> dimensions = dam.getDimensions();
				long nbBitPerElt = getBitSize(dam.getLogicalArrayType());

				Long nbElt = dimensions.stream().reduce(1L, (a, b) -> a * b);
				long arrayTotalSize = nbElt * nbBitPerElt / 8;


				List<Long> etpMsg_Ids = new ArrayList<>();
				logger.info("nbElt " + nbElt + " SIZE is =>" + arrayTotalSize + " bytes == " + dimensions.stream().map(x -> x + "").reduce("", (a,b) -> a + " " + b));

				long step = Math.min(dimensions.get(0) * nbBitPerElt / 8, // Size of the array for one element (only for the first dimension)
						ETPClient.MAX_PAYLOAD_SIZE * 8 / nbBitPerElt ); // Max<nb of element possible to send
				// On divise par les autres dimensions pour avoir un element entier (ex les coord (x, y, z) pour un point)
				for(int i=1; i<dimensions.size(); i++){
					step = step / dimensions.get(i);
				}

				for(long subArrStart=0; subArrStart<dimensions.get(0); subArrStart += step) {
//					Map<CharSequence, GetDataSubarraysType> mapSubArrType = new HashMap<>();
//					mapSubArrType.put("0", GetDataSubarraysType.newBuilder()
//							.setUid(identifier)
//							.setStarts(Arrays.asList(new Long[]{subArrStart}))
//							.setCounts(Arrays.asList(new Long[]{Math.min(dimensions.get(0)-subArrStart, step)}))
//							.build());
//					logger.info("\t Start " + mapSubArrType.get("0").getStarts() + " \t Counts => "+ mapSubArrType.get("0").getCounts());
//
//					GetDataSubarrays gsa = GetDataSubarrays.newBuilder()
//							.setDataSubarrays(mapSubArrType)
//							.build();
//					etpMsg_Ids.add(etpClient.send(gsa));

					Long[] start = new Long[dimensions.size()];
					Long[] count = new Long[dimensions.size()];

					start[0] = subArrStart;
					count[0] = Math.min(dimensions.get(0)-subArrStart, step);


					for(int dim_idx=1; dim_idx<dimensions.size(); dim_idx++){
						start[dim_idx] = 0L;
						count[dim_idx] = dimensions.get(dim_idx);
					}
					etpMsg_Ids.add(sendGetDataSubArray_nowait(etpClient, uri, pathInHDF5, start, count));
				}
				for(Long msgId: etpMsg_Ids){
					result.addAll(etpClient.getEtpClientSession().waitForResponse(msgId, timeoutMS));
				}
			}
		}catch (Exception e){
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
		return result;
	}

	public static Long sendGetDataSubArray_nowait(ETPClient etpClient, String uri, String pathInHDF5, Long[] start, Long[] count){
		DataArrayIdentifier identifier = DataArrayIdentifier.newBuilder()
				.setUri(uri)
				.setPathInResource(pathInHDF5)
				.build();

		Map<CharSequence, GetDataSubarraysType> mapSubArrType = new HashMap<>();
		mapSubArrType.put("0", GetDataSubarraysType.newBuilder()
				.setUid(identifier)
				.setStarts(Arrays.asList(start))
				.setCounts(Arrays.asList(count))
				.build());

		GetDataSubarrays gdsa = GetDataSubarrays.newBuilder()
				.setDataSubarrays(mapSubArrType)
				.build();
		long msgId = etpClient.send(gdsa);
		logger.info(msgId + ") GetDataSubArray : on" + pathInHDF5
				+ "start [ " + Arrays.stream(start).map(a -> a + " ").reduce("", (a, b) -> a+b) + "]"
				+ " count [ " + Arrays.stream(count).map(a -> a + " ").reduce("", (a,b) -> a+b) + "]");

		return msgId;
	}

	public static long getBitSize(AnyLogicalArrayType type){
		switch (type){
    		case arrayOfInt8:
    		case arrayOfUInt8:
				return 8L;
    		case arrayOfInt16LE:
    		case arrayOfUInt16LE:
    		case arrayOfInt16BE:
    		case arrayOfUInt16BE:
    		case arrayOfString:
				return 16L;
    		case arrayOfUInt32BE:
    		case arrayOfInt32LE:
    		case arrayOfUInt32LE:
    		case arrayOfFloat32LE:
    		case arrayOfInt32BE:
    		case arrayOfFloat32BE:
				return 32L;
    		case arrayOfUInt64LE:
    		case arrayOfInt64LE:
    		case arrayOfDouble64LE:
    		case arrayOfInt64BE:
    		case arrayOfUInt64BE:
    		case arrayOfDouble64BE:
				return 64L;
    		case arrayOfCustom:
			case arrayOfBoolean:
		}
		return 1L;
	}

	public static List<Message> sendGetDataSubArray(ETPClient etpClient, String uri, String pathInHDF5, Long[] start, Long[] count, int timeoutMS){
		return etpClient.getEtpClientSession().waitForResponse(sendGetDataSubArray_nowait(etpClient, uri, pathInHDF5, start, count), timeoutMS);
	}

	public static List<Message> sendGetDataArray(ETPClient etpClient, String uri, String pathInHDF5, int timeoutMS){
		DataArrayIdentifier identifier = DataArrayIdentifier.newBuilder()
				.setUri(uri)
				.setPathInResource(pathInHDF5)
				.build();

		Map<CharSequence, DataArrayIdentifier> mapIdentifier = new HashMap<>();
		mapIdentifier.put("0", identifier);

		GetDataArrays gda = GetDataArrays.newBuilder()
				.setDataArrays(mapIdentifier).build();
		long msg_id = etpClient.send(gda);
		logger.info(msg_id + ") GetDataSubArray sent");
		return etpClient.getEtpClientSession().waitForResponse(msg_id, timeoutMS);
	}

	public static List<AnyArray> sendGetDataArray_pretty(ETPClient etpClient, String uri, String pathInHDF5, int timeoutMS, boolean useSubArrays){
		List<AnyArray> result = new ArrayList<>();

		List<Message> msgs;
		if(useSubArrays){
			msgs = sendGetDataArray_withSubArray(etpClient, uri, pathInHDF5, timeoutMS);
		}else{
			msgs = sendGetDataArray(etpClient, uri, pathInHDF5, timeoutMS);
		}

		for(Message msg : msgs) {
			if (msg.getBody() instanceof GetDataArraysResponse) {
				GetDataArraysResponse objResp = (GetDataArraysResponse) msg.getBody();
				if (objResp.getDataArrays().size() > 0) {
					for(CharSequence s : objResp.getDataArrays().keySet()) {
						DataArray firstArray = objResp.getDataArrays().get(s);
						result.add(firstArray.getData());
					}
				}
			}else if(msg.getBody() instanceof GetDataSubarraysResponse){
				GetDataSubarraysResponse objResp = (GetDataSubarraysResponse) msg.getBody();
				if (objResp.getDataSubarrays().size() > 0) {
					for(CharSequence s : objResp.getDataSubarrays().keySet()) {
						DataArray firstArray = objResp.getDataSubarrays().get(s);
						result.add(firstArray.getData());
					}
				}
			}
		}
		return result;
	}

	public static List<Number> sendGetDataArray_prettier(ETPClient etpClient, String uri, String pathInHDF5, int timeoutMS, boolean useSubArrays){
		List<AnyArray> allArrays = ETPHelper.sendGetDataArray_pretty(etpClient, uri,pathInHDF5, timeoutMS, useSubArrays);
		List<Number> allPoints = new ArrayList<>();
		for(AnyArray ar: allArrays){
			try {
				allPoints.addAll((List<Number>) Objects.requireNonNull(ETPUtils.getAttributeValue(Objects.requireNonNull(ETPUtils.getAttributeValue(ar, "item")), "values")));
			}catch (Exception e){
				logger.error(e.getMessage());
				logger.debug(e.getMessage(), e);
			}
		}
		return allPoints;
	}

	/* PutDataArray */

	public static List<Message> sendPutDataArray(ETPClient etpClient, String uri, String pathInHDF5, DataArray da, int timeoutMS){
		Map<CharSequence, PutDataArraysType> map = new HashMap<>();
		DataArrayIdentifier dai = DataArrayIdentifier.newBuilder()
				.setUri(uri)
				.setPathInResource(pathInHDF5)
				.build();
		map.put("0", PutDataArraysType.newBuilder()
				.setUid(dai)
				.setArray(da)
				.build()
		);
		PutDataArrays msg = PutDataArrays.newBuilder()
				.setDataArrays(map).build();
		long msg_id = etpClient.send(msg);
		return etpClient.getEtpClientSession().waitForResponse(msg_id, timeoutMS);
	}

}
