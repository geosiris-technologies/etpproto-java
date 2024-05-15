package com.geosiris.etp.utils;

import Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArray;
import Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayIdentifier;
import Energistics.Etp.v12.Datatypes.Object.*;
import Energistics.Etp.v12.Protocol.DataArray.GetDataArrays;
import Energistics.Etp.v12.Protocol.DataArray.GetDataArraysResponse;
import Energistics.Etp.v12.Protocol.DataArray.PutDataArraysResponse;
import Energistics.Etp.v12.Protocol.Dataspace.GetDataspaces;
import Energistics.Etp.v12.Protocol.Dataspace.GetDataspacesResponse;
import Energistics.Etp.v12.Protocol.Dataspace.PutDataspaces;
import Energistics.Etp.v12.Protocol.Dataspace.PutDataspacesResponse;
import Energistics.Etp.v12.Protocol.Discovery.GetResources;
import Energistics.Etp.v12.Protocol.Discovery.GetResourcesResponse;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.websocket.ETPClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ETPHelperREST {
	public static final Logger logger = LogManager.getLogger(ETPHelperREST.class);

    public final static String DATA_ARRAY = "data-array";
    public final static String DATASPACE = "dataspace";
    public final static String DISCOVERY = "discovery";
    public final static String STORE = "store";

    public static GetDataArraysResponse getMultipleDataArrays(ETPClient client, String uri, List<String> datasets_paths) throws Exception {
        return getMultipleDataArrays(client.getServerUri().toString(), uri, datasets_paths);
    }

    public static GetDataArraysResponse getMultipleDataArrays(String rootUrl, String uri, List<String> datasets_paths) throws Exception {
        rootUrl = getHTTPUrl(rootUrl) + "/" + DATA_ARRAY + "/get";

        Map<CharSequence, DataArrayIdentifier> mapIdentifier = new HashMap<>();
        for(String ds_path: datasets_paths){
            DataArrayIdentifier identifier = DataArrayIdentifier.newBuilder()
                    .setUri(uri)
                    .setPathInResource(ds_path)
                    .build();
            mapIdentifier.put(ds_path, identifier);
        }
        return sendReq(rootUrl, GetDataArrays.newBuilder().setDataArrays(mapIdentifier).build(), new GetDataArraysResponse());
    }

    public static PutDataArraysResponse putDataArrays(String rootUrl, String uri, List<Pair<String, DataArray>> arrays) throws Exception {
        rootUrl = getHTTPUrl(rootUrl) + "/" + DATA_ARRAY + "/put";
        return sendReq(rootUrl, ETPHelper.buildPutDataArray(uri, arrays), new PutDataArraysResponse());
    }

    public static GetDataspacesResponse getDataspaces(ETPClient client) throws Exception {
        return getDataspaces(client.getServerUri().toString());
    }

    public static GetDataspacesResponse getDataspaces(String rootUrl) throws Exception {
        rootUrl = getHTTPUrl(rootUrl) + "/" + DATASPACE + "/get";
        return sendReq(rootUrl, new GetDataspaces(), new GetDataspacesResponse());
    }

    public static PutDataspacesResponse putDataspaces(ETPClient client, String dataspaceName) throws Exception {
        return putDataspaces(client.getServerUri().toString(), dataspaceName);
    }

    public static PutDataspacesResponse putDataspaces(String rootUrl, String dataspaceName) throws Exception {
        rootUrl = getHTTPUrl(rootUrl) + "/" + DATASPACE + "/put";
        return sendReq(rootUrl, PutDataspaces.newBuilder()
                .setDataspaces(Map.of("0",
                        Dataspace.newBuilder()
                                .setUri(new ETPUri(dataspaceName).toString())
                                .setPath("")
                                .setStoreCreated(0)
                                .setStoreLastWrite(0)
                                .setCustomData(new HashMap<>())
                                .build()
                        ))
                .build(),
                new PutDataspacesResponse());
    }

    public static GetResourcesResponse getResources(ETPClient client,  String uri, ContextScopeKind scope) throws Exception {
        return getResources(client.getServerUri().toString(), uri, scope);
    }

    public static GetResourcesResponse getResources(String rootUrl, String uri, ContextScopeKind scope) throws Exception {
        rootUrl = getHTTPUrl(rootUrl) + "/" + DISCOVERY + "/get";
        return sendReq(rootUrl, GetResources.newBuilder()
                .setScope(scope)
                .setStoreLastWriteFilter(null)
                .setCountObjects(false)
                .setIncludeEdges(false)
                .setContext(
                        ContextInfo.newBuilder()
                                .setUri(uri)
                                .setDepth(1)
                                .setNavigableEdges(RelationshipKind.Both)
                                .setIncludeSecondarySources(true)
                                .setIncludeSecondarySources(true)
                                .setDataObjectTypes(new ArrayList<>())
                                .build()
                )
                .setActiveStatusFilter(null)
                .build(),
                new GetResourcesResponse());
    }

    public static <T extends SpecificRecordBase> T sendReq(String rootUrl, SpecificRecordBase req, T res) throws Exception {
        return sendReq(rootUrl, new ArrayList<>(), req, res);
    }

    public static <T extends SpecificRecordBase> T sendReq(String rootUrl, List<Pair<String, String>> headers, SpecificRecordBase req, T res) throws Exception {
        String data = Message.encodeJson(req);
        HttpClient httpclient = new HttpClient();
        logger.info("Sending data to {}", rootUrl);
        Request request = httpclient.POST(rootUrl);
        for(Pair<String, String> head: headers){
            request.header(head.l(), head.r());
        }
        request.content(new StringContentProvider(data), "application/json");
        logger.debug("Sending {}", data);

        T result = null;
        try {
            httpclient.start();
            ContentResponse resp = request.send();
                logger.error(resp.getStatus());
            if(resp.getStatus() == 200) {
                String reqAnswerContent = resp.getContentAsString();
                logger.debug(reqAnswerContent);
                result = (T) Message.decodeJson(reqAnswerContent, res);
            }else {
                logger.error(resp.getContentAsString());
            }
        }finally {
            httpclient.stop();
        }
        return result;
    }

    public static String getUrlFromClient(ETPClient client){
       return getHTTPUrl(client.getServerUri().toString());
    }

    public static String getHTTPUrl(String rootUrl){
        if(!rootUrl.toLowerCase().startsWith("http")){
            rootUrl = rootUrl.replaceFirst("[^:]+:", "http:");
        }
        return rootUrl;
    }
}
