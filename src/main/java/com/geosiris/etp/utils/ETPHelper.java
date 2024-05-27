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

import Energistics.Etp.v12.Datatypes.*;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

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
//			logger.debug(pdi_id + ") Waiting for Put data answer");
			return etpClient.getEtpClientSession().waitForResponse(pdi_id, timeoutMS);
//			logger.debug(pdi_id + ") Answer : " + dataResp);
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

//		logger.debug(getData_id+") Waiting for GetDataObjects answer\n" + getDataO);
		return etpClient.getEtpClientSession().waitForResponse(getData_id, timeoutMS);
//		logger.debug(getData_id+") Answer : " + dataResp);
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
//		logger.debug(getRess_id+") Waiting for GetResources answer");
		//		logger.debug(getRess_id+") Answer : " + ressResp_l);

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
//				logger.debug("Found objects uri : ");
//				logger.debug("Nb ressources found : " + objResp.getResources().size());
				for (Resource res : objResp.getResources()) {
//						logger.debug(res.getUri());
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

	public static GetDataArrayMetadataResponse getMetaData(ETPClient client, String uri, List<String> datasets_paths, int timeoutMS){
		GetDataArrayMetadataResponse result = null;

		Map<CharSequence, DataArrayIdentifier> mapIdentifier = new HashMap<>();
		for(String ds_path: datasets_paths) {
			DataArrayIdentifier identifier = DataArrayIdentifier.newBuilder()
					.setUri(uri)
					.setPathInResource(ds_path)
					.build();
			mapIdentifier.put(ds_path, identifier);
		}

		GetDataArrayMetadata meta = GetDataArrayMetadata.newBuilder()
				.setDataArrays(mapIdentifier)
				.build();
		long meta_msg_id = client.send(meta);
		logger.debug(meta);
		List<Message> meta_resp = client.getEtpClientSession().waitForResponse(meta_msg_id, timeoutMS);
		for(Message m: meta_resp){
			if(m.getBody() instanceof GetDataArrayMetadataResponse){
				if(result == null){
					result = (GetDataArrayMetadataResponse) m.getBody();
				}else{
					result.getArrayMetadata().putAll(((GetDataArrayMetadataResponse) m.getBody()).getArrayMetadata());
				}
			}
		}
		return result;
	}

	public static Map<String, List<Number>> getMultipleDataArrays(ETPClient client, String uri, List<String> datasets_paths, int timeoutMS){
		Map<String, List<Number>> result = new HashMap<>();
		final int securitySize = 200; // For the size of the message data (other than array values) # it is a
		final Long eltSize = 64L;

		logger.debug("Loading data for {}", uri);
		logger.debug("[");
		for(String p: datasets_paths){
			logger.debug("\t{}, ", p);
		}
		logger.debug("]");

		long maxMsgSize = client.getMaxMsgSize();

		// Getting metadata
		GetDataArrayMetadataResponse metas = getMetaData(client, uri, datasets_paths, timeoutMS);
		logger.debug("Metadatas Received : {}", metas);
		List<Pair<Long, List<String>>> fullArrays = new ArrayList<>();
		Map<String, DataArrayMetadata> subArrays = new HashMap<>(); // path -> size
		if(metas != null){

			for(Map.Entry<CharSequence, DataArrayMetadata> e: metas.getArrayMetadata().entrySet()){
				long thisSize = e.getValue().getDimensions().stream().reduce(1L, (a, b) -> a * b) * eltSize;

				if(maxMsgSize > thisSize){
					boolean added = false;
					for(Pair<Long, List<String>> fulls: fullArrays){
						if(maxMsgSize > fulls.l() + thisSize){
							added = true;
							List<String> l_c = new ArrayList<>(fulls.r());
							l_c.add(e.getKey().toString());
							fullArrays.add(new Pair<>(fulls.l() + thisSize, l_c));
							fullArrays.remove(fulls);
							break;
						}
					}
					if(!added){
						fullArrays.add(new Pair<>(securitySize + thisSize, List.of(e.getKey().toString())));
					}
				}else{ // Too large for one msg
					subArrays.put(e.getKey().toString(), e.getValue());
				}
			}
		}
		logger.debug("Arrays");
		for(Pair<Long, List<String>> fk: fullArrays){
			logger.debug("\t" + fk.l() + "[" + String.join(", ", fk.r()) + "]");
		}
		logger.debug("SubArrays");
		logger.debug("\t[{}]", String.join(",\n", subArrays.keySet()));

		assert subArrays.size() + fullArrays.stream().map(x -> x.r().size()).reduce(0, (a,b) -> a + b) == datasets_paths.size();

		List<Long> msgIdsToWait = new ArrayList<>();
		for(Pair<Long, List<String>> fk: fullArrays){
			List<String> paths = fk.r();
			Map<CharSequence, DataArrayIdentifier> mapIdentifier = new HashMap<>();
			for(String ds_path: paths){
				DataArrayIdentifier identifier = DataArrayIdentifier.newBuilder()
						.setUri(uri)
						.setPathInResource(ds_path)
						.build();
				mapIdentifier.put(ds_path, identifier);
			}
			GetDataArrays gda = GetDataArrays.newBuilder().setDataArrays(mapIdentifier).build();
			long msgId = client.send(gda);
			msgIdsToWait.add(msgId);
		}

		// Sub Arrays
		Map<Long, String> msgIdsToWaitSubArray = new HashMap<>();
		for(Map.Entry<String, DataArrayMetadata> e_subArray: subArrays.entrySet()){
			List<Long> dimensions = e_subArray.getValue().getDimensions();
			Map<CharSequence, GetDataSubarraysType> mapSubArr = new HashMap<>();

			long startLineIdx = 0;
			long endLineIdx = 0;
			long sizeOfLine = dimensions.size() > 1 ? dimensions.subList(1, dimensions.size()).stream().reduce(1L, (a, b) -> a * b) : 1;
			long nbLinePerMsg = (maxMsgSize - securitySize) / (sizeOfLine * eltSize);
			while(endLineIdx < dimensions.get(0)){
				endLineIdx = Math.min(startLineIdx + nbLinePerMsg, dimensions.get(0));
				List<Long> counts = new ArrayList<>();
				List<Long> starts = new ArrayList<>();

				counts.add(endLineIdx - startLineIdx);
				starts.add(startLineIdx);

				for(int i=1; i<dimensions.size(); i++){
					counts.add(dimensions.get(i));
					starts.add(0L);
				}
				logger.debug("SubArr : Counts {} : Starts {}", counts , starts);
				mapSubArr.put(String.valueOf(mapSubArr.size()),
						GetDataSubarraysType.newBuilder()
								.setCounts(counts)
								.setStarts(starts)
								.setUid(DataArrayIdentifier.newBuilder()
										.setUri(uri)
										.setPathInResource(e_subArray.getKey())
										.build()
								).build()
				);
				GetDataSubarrays gdsa = GetDataSubarrays.newBuilder()
						.setDataSubarrays(mapSubArr).build();
				long msgId = client.send(gdsa);
				msgIdsToWaitSubArray.put(msgId, e_subArray.getKey());

				startLineIdx = startLineIdx + nbLinePerMsg;
			}
		}

		Map<String, List<Message>> subArrayResps = new HashMap<>();

		for(Map.Entry<Long, List<Message>> e_answers: client.getEtpClientSession().waitForResponse(msgIdsToWait, 500000).entrySet()){
			List<Message> answers = e_answers.getValue();
			for(Message answer: answers.stream().sorted(Comparator.comparingInt(m0 -> (int) m0.getHeader().getMessageId())).collect(Collectors.toList())) {
				if (answer.getBody() instanceof GetDataArraysResponse) {
					GetDataArraysResponse gdar = ((GetDataArraysResponse) answer.getBody());
					for(Map.Entry<CharSequence, DataArray> e_gdar: gdar.getDataArrays().entrySet()){
						try {
							result.put(e_gdar.getKey().toString(), (List<Number>) ETPUtils.getAttributeValue(e_gdar.getValue().getData().getItem(), "values"));
						} catch (Exception e) {
							logger.error("Err for {}", e_gdar.getKey());
							logger.error(e);
						}

					}
				} else {
					logger.debug("Unexpected answer for msg : {}", answer.getHeader().getCorrelationId());
				}
			}
		}

		for(Map.Entry<Long, List<Message>> e_answers: client.getEtpClientSession().waitForResponse(new ArrayList<>(msgIdsToWaitSubArray.keySet()), 500000).entrySet()){
			List<Message> answers = e_answers.getValue();
			for(Message answer: answers.stream().sorted(Comparator.comparingInt(m0 -> (int) m0.getHeader().getMessageId())).collect(Collectors.toList())) {
				if (answer.getBody() instanceof GetDataSubarraysResponse) {
					String identifier = msgIdsToWaitSubArray.get(e_answers.getKey());
					logger.debug("identifier : {}", identifier);
					logger.debug(String.join(", \n", ((GetDataSubarraysResponse) answer.getBody()).getDataSubarrays().keySet()));
					if(!subArrayResps.containsKey(identifier)){
						subArrayResps.put(identifier, new ArrayList<>());
					}
//					subArrayResps.get(identifier).add(((GetDataSubarraysResponse) answer.getBody()));
					subArrayResps.get(identifier).add(answer);

				} else {
					logger.debug("Unexpected answer for msg : {}", answer.getHeader().getCorrelationId());
				}
			}
		}

		// For subArraysResponse : sort and reduce results for each array
		for(Map.Entry<String, List<Message>> sub_arr_r: subArrayResps.entrySet()){
			/*Map<CharSequence, DataArray> allSubArrayParts =
					sub_arr_r.getValue().stream()
							.map(m_gdsr -> ((GetDataSubarraysResponse)m_gdsr.getBody()).getDataSubarrays().entrySet()).flatMap(Set::stream)
							.collect(Collectors.toMap(Map.Entry<CharSequence, DataArray>::getKey, Map.Entry<CharSequence, DataArray>::getValue));
			List<Number> numbers = allSubArrayParts.entrySet().stream()
					.sorted(Comparator.comparingInt(e -> Integer.getInteger(e.getKey().toString())))
					.map(e -> {
                        try {
                            return (List<Number>) ETPUtils.getAttributeValue(e.getValue().getData().getItem(), "values");
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    })
					.flatMap(List::stream).collect(Collectors.toList());
			result.put(sub_arr_r.getKey(),numbers);*/
			List<Number> das = sub_arr_r.getValue().stream()
					.sorted(Comparator.comparingLong(m0 -> m0.getHeader().getMessageId()))
					.map(m -> ((GetDataSubarraysResponse) m.getBody()).getDataSubarrays().values())
					.flatMap(Collection::stream)
					.map(e -> {
						try {
							return (List<Number>) ETPUtils.getAttributeValue(e.getData().getItem(), "values");
						} catch (Exception ex) {
							throw new RuntimeException(ex);
						}
					}).filter(Objects::nonNull)
					.flatMap(List::stream)
					.collect(Collectors.toList());
			result.put(sub_arr_r.getKey(), das);
		}

		return result;
	}


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
				logger.debug("META>" + meta_response);

				DataArrayMetadata dam = meta_response.getArrayMetadata().values().iterator().next();
				List<Long> dimensions = dam.getDimensions();
				long nbBitPerElt = getBitSize(dam.getLogicalArrayType());

				Long nbElt = dimensions.stream().reduce(1L, (a, b) -> a * b);
				long arrayTotalSize = nbElt * nbBitPerElt / 8;


				List<Long> etpMsg_Ids = new ArrayList<>();
				logger.debug("nbElt " + nbElt + " SIZE is =>" + arrayTotalSize + " bytes == " + dimensions.stream().map(x -> x + "").reduce("", (a,b) -> a + " " + b));

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
//					logger.debug("\t Start " + mapSubArrType.get("0").getStarts() + " \t Counts => "+ mapSubArrType.get("0").getCounts());
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

	public static void sendPutDataArray(ETPClient etpClient, String uri, String pathInResource, List<?> dataFlat, List<Long> dimensions, AnyLogicalArrayType alat, AnyArrayType aat, int maxSize){
		Map<CharSequence, PutDataSubarraysType> map = new HashMap<>();
		PutUninitializedDataArrays pud = PutUninitializedDataArrays.newBuilder()
				.setDataArrays(
						Map.of(
								"0", PutUninitializedDataArrayType.newBuilder()
										.setUid(DataArrayIdentifier.newBuilder()
												.setPathInResource(pathInResource)
												.setUri(uri)
												.build()
										).setMetadata(
												DataArrayMetadata.newBuilder()
														.setDimensions(dimensions)
														.setStoreCreated(new Date().getTime())
														.setStoreLastWrite(new Date().getTime())
														.setLogicalArrayType(alat)
														.setCustomData(new HashMap<>())
														.setTransportArrayType(aat)
														.build()
										).build()
						)
				).build();
		long pud_id = etpClient.send(pud);
		List<Message> pud_resp = etpClient.getEtpClientSession().waitForResponse(pud_id, 50000);

		long nbBitPerElt = getBitSize(alat);
		Long nbEltPerLine = dimensions.subList(1, dimensions.size()).stream().reduce(1L, (a, b) -> a * b);
		Long nbLinePerSplit = maxSize / (nbEltPerLine * nbBitPerElt);

		final long nbLineInData = dataFlat.size() / nbEltPerLine;

		for(long li=0; li<nbLineInData; li+=nbLinePerSplit) {
			long first =  (li*nbEltPerLine);
			long last = Math.min(dataFlat.size(), (int) ((li + nbLinePerSplit)*nbEltPerLine));

			List<Long> counts = new ArrayList<>();
			counts.add((last - first) / nbEltPerLine);
			counts.addAll(dimensions.subList(1, dimensions.size()));

			List<Long> starts = new ArrayList<>();
			starts.add(li);
			for(int di=1; di<dimensions.size(); di++){
				starts.add(0L);
			}

			System.out.print("starts [");
			starts.forEach(s -> System.out.print(s + ", "));
			System.out.print("]\n");
			System.out.print("counts[");
			counts.forEach(s -> System.out.print(s + ", "));
			System.out.print("]\n");
			AnyArray aa = constructArray(dataFlat.subList((int) (li*nbEltPerLine), Math.min(dataFlat.size(), (int) ((li + nbLinePerSplit)*nbEltPerLine))));
			DataArrayIdentifier dai = DataArrayIdentifier.newBuilder()
					.setUri(uri)
					.setPathInResource(pathInResource)
					.build();
			map.put(String.valueOf(map.size()), PutDataSubarraysType.newBuilder()
					.setUid(dai)
					.setData(aa)
					.setStarts(starts)
					.setCounts(counts)
					.build()
			);
		}
		PutDataSubarrays msg = PutDataSubarrays.newBuilder()
				.setDataSubarrays(map).build();
		long msg_id = etpClient.send(msg);
		logger.info("Exporting : PP " + uri + "  " + pathInResource +  " msg id : " + msg_id + "\n");

		List<Message> pdar = etpClient.getEtpClientSession().waitForResponse(msg_id, 50000);
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
		logger.debug(msgId + ") GetDataSubArray : on" + pathInHDF5
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
		logger.debug(msg_id + ") GetDataSubArray sent");
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

	public static PutDataArrays buildPutDataArray(String uri, List<Pair<String, DataArray>> das){
		try {
			Map<CharSequence, PutDataArraysType> map = new HashMap<>();
			for (Pair<String, DataArray> pathAndDa : das) {
				DataArrayIdentifier dai = DataArrayIdentifier.newBuilder()
						.setUri(uri)
						.setPathInResource(pathAndDa.l())
						.build();
				map.put(String.valueOf(map.size()), PutDataArraysType.newBuilder()
						.setUid(dai)
						.setArray(pathAndDa.r())
						.setCustomData(new HashMap<>())
						.build()
				);
			}
			return PutDataArrays.newBuilder()
					.setDataArrays(map).build();
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

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

	public static AnyArray constructArray(List<?> data){
        if (data.get(0).getClass().equals(Double.class)) {
            return AnyArray.newBuilder()
                    .setItem(ArrayOfDouble.newBuilder()
                            .setValues((List<Double>) data)
                            .build()
                    ).build();
        } else if (data.get(0).getClass().equals(Integer.class)) {
            return AnyArray.newBuilder()
                    .setItem(ArrayOfInt.newBuilder()
                            .setValues((List<Integer>) data)
                            .build()
                    ).build();
        } else if (data.get(0).getClass().equals(Long.class)) {
            return AnyArray.newBuilder()
                    .setItem(ArrayOfLong.newBuilder()
                            .setValues((List<Long>) data)
                            .build()
                    ).build();
        } else if (data.get(0).getClass().equals(Float.class)) {
            return AnyArray.newBuilder()
                    .setItem(ArrayOfFloat.newBuilder()
                            .setValues((List<Float>) data)
                            .build()
                    ).build();
        } else if (data.get(0).getClass().equals(String.class)) {
            return AnyArray.newBuilder()
                    .setItem(ArrayOfString.newBuilder()
                            .setValues((List<CharSequence>) data)
                            .build()
                    ).build();
        } else if (data.get(0).getClass().equals(Boolean.class)) {
            return AnyArray.newBuilder()
                    .setItem(ArrayOfBoolean.newBuilder()
                            .setValues((List<Boolean>) data)
                            .build()
                    ).build();
        }
        logger.error("Not supported type for any array {}", data);
        if (!data.isEmpty())
            logger.error("\t" + data.get(0));
        return null;

    }

	public static void main(String[] argv){
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns2:TriangulatedSetRepresentation xmlns=\"http://www.energistics.org/energyml/data/commonv2\" xmlns:ns2=\"http://www.energistics.org/energyml/data/resqmlv2\" uuid=\"f9a85fba-7f74-401f-99a2-03ff00a50086\" schemaVersion=\"2.2\" objectVersion=\"0\"><Citation><Title>null[Simplified]</Title><Creation>2023-10-27T17:04:25.403+02:00</Creation><LastUpdate>2023-10-27T17:04:25.404+02:00</LastUpdate><Description>A simplification of the representation null realised by Jerboa (a tool from University of Poitiers [FRANCE])</Description></Citation><ns2:TrianglePatch><ns2:NodeCount>10755</ns2:NodeCount><ns2:Triangles xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"IntegerExternalArray\"><NullValue>0</NullValue><CountPerValue>1</CountPerValue><Values><ExternalDataArrayPart><PathInExternalFile>RESQML/f9a85fba-7f74-401f-99a2-03ff00a50086triangle_patch0</PathInExternalFile></ExternalDataArrayPart></Values></ns2:Triangles><ns2:Geometry><ns2:Points xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"ns2:Point3dExternalArray\"><ns2:Coordinates><ExternalDataArrayPart><PathInExternalFile>RESQML/f9a85fba-7f74-401f-99a2-03ff00a50086point_patch0</PathInExternalFile></ExternalDataArrayPart></ns2:Coordinates></ns2:Points></ns2:Geometry></ns2:TrianglePatch> </ns2:TriangulatedSetRepresentation>";
		Document xmlDoc = null;
		try {
			xmlDoc = XmlUtils.readXml(xml);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		System.out.println(XmlUtils.xml_getSubNodecontent(xmlDoc, "Citation.Title"));
	}

}
