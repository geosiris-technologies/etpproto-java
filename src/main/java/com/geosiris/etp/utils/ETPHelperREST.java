package com.geosiris.etp.utils;

import Energistics.Etp.v12.Datatypes.DataArrayTypes.DataArrayIdentifier;
import Energistics.Etp.v12.Protocol.DataArray.GetDataArrays;
import Energistics.Etp.v12.Protocol.DataArray.GetDataArraysResponse;
import com.geosiris.etp.communication.Message;
import com.geosiris.etp.websocket.ETPClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ETPHelperREST {
	public static final Logger logger = LogManager.getLogger(ETPHelperREST.class);

    public final static String DATA_ARRAY = "data-array";

    public static GetDataArraysResponse getMultipleDataArrays(ETPClient client, String uri, List<String> datasets_paths) throws Exception {
        return getMultipleDataArrays(client.getServerUri().toString(), uri, datasets_paths);
    }

    public static GetDataArraysResponse getMultipleDataArrays(String rootUrl, String uri, List<String> datasets_paths) throws Exception {
        if(!rootUrl.toLowerCase().startsWith("http")){
            rootUrl = rootUrl.replaceFirst("[^:]+:", "http:");
        }
        rootUrl += "/" + DATA_ARRAY + "/get";
        GetDataArraysResponse resp = null;

        Map<CharSequence, DataArrayIdentifier> mapIdentifier = new HashMap<>();
        for(String ds_path: datasets_paths){
            DataArrayIdentifier identifier = DataArrayIdentifier.newBuilder()
                    .setUri(uri)
                    .setPathInResource(ds_path)
                    .build();
            mapIdentifier.put(ds_path, identifier);
        }
        GetDataArrays gda = GetDataArrays.newBuilder().setDataArrays(mapIdentifier).build();
        String data = Message.encodeJson(gda);

        HttpClient httpclient = new HttpClient();
        logger.debug("Sending data to {}", rootUrl);
        Request req = httpclient.POST(rootUrl);

        req.content(new StringContentProvider(data), "application/json");
        httpclient.start();
        String reqAnswercontent = req.send().getContentAsString();
        logger.debug(reqAnswercontent);
        GetDataArraysResponse result = (GetDataArraysResponse) Message.decodeJson(reqAnswercontent, new GetDataArraysResponse());
        httpclient.stop();
        return result;
    }
}
