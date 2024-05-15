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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class XmlUtils {
	public static final Logger logger = LogManager.getLogger(XmlUtils.class);

	private static DocumentBuilder db;

	static {try {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setNamespaceAware(true);
		db = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {e.printStackTrace();}}

	public static Document readXml(String fileContent) throws IOException, SAXException {
		return db.parse(new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8)));
	}

	public static String searchUuidInXml(Document fileContent){
		try {
			return fileContent.getFirstChild().getAttributes().getNamedItem("uuid").getNodeValue();
		}catch (Exception e){
			logger.debug(e.getMessage(), e);
			return null;
		}
	}

	public static String searchTypeInXml(Document fileContent){
		try {
			return fileContent.getFirstChild().getLocalName();
		}catch (Exception e){
			logger.debug(e.getMessage(), e);
			return null;
		}
	}

	public static String getEnergymlTypeInXml(Document fileContent) {
		String rootNamespace = fileContent.getFirstChild().getNamespaceURI().toLowerCase();
		logger.debug("namespace " + rootNamespace);
		if(rootNamespace.matches(".*/resqml(v2)?$")){
			return "resqml";
		}else if(rootNamespace.matches(".*/witsml(v2)?$")){
			return "witsml";
		}else if(rootNamespace.matches(".*/prodml(v2)?$")){
			return "prodml";
		}else if(rootNamespace.matches(".*/common(v2)?$")){
			return "eml";
		}
		return "unknown";
	}

	public static String getSchemaVersion(Document fileContent){
		String sv = fileContent.getFirstChild().getAttributes().getNamedItem("schemaVersion").getNodeValue();
		Pattern pSV = Pattern.compile("(?<version>[\\d]+([\\._][\\d]+)*)");
		Matcher match = pSV.matcher(sv);
		if(match.find()){
			return match.group("version");
		}
		return null;
	}

	public static String getEnergymlNamespaceForETP(Document fileContent){
		return getEnergymlTypeInXml(fileContent) + Objects.requireNonNull(getSchemaVersion(fileContent)).replaceAll("[_\\.]+", "").substring(0,2);
	}

	public static String getUriInXml(Document fileContent, String dataspace){
		String dataspaceInURI = "";
		if(dataspace != null && dataspace.length()>0){
			dataspaceInURI = "dataspace(" + dataspace + ")/";
		}
		return "eml:///" + dataspaceInURI + getEnergymlNamespaceForETP(fileContent) + "." + searchTypeInXml(fileContent) + "(" + searchUuidInXml(fileContent) + ")";
	}
	public static String xml_getSubNodecontent(Document fileContent, String dotPath){
		Node currentNode = fileContent.getFirstChild();
		String lastName = "";
		boolean resultIsAttribute = false;

		while(dotPath.length()>0){
			String currentName = dotPath;
			boolean hadDot = false;
			if(currentName.contains(".")){
				hadDot = true;
				currentName = currentName.substring(0,dotPath.indexOf("."));
			}
			dotPath = dotPath.substring(currentName.length() + (hadDot?1:0));
			if(currentName.length()>0){
				NodeList children = currentNode.getChildNodes();
				if(Integer.getInteger(currentName)!=null){ // index de list
					int idxToSearch = Integer.getInteger(currentName);
					int cpt = 0;
					for(int i=0; i<children.getLength(); i++){
						Node child = children.item(i);
						if(child.getLocalName().compareToIgnoreCase(lastName) == 0){
							if(cpt == idxToSearch){
								currentNode = child;
								break;
							}else{
								cpt++;
							}
						}
					}
				}else{
					boolean found = false;
					for(int i=0; i<children.getLength(); i++){
						Node child = children.item(i);
						if(child.getLocalName().compareToIgnoreCase(currentName) == 0){
							currentNode = child;
							found = true;
							break;
						}
					}
					if(!found){
						resultIsAttribute = true;
						NamedNodeMap attribMap = currentNode.getAttributes();
						for(int i=0; i<attribMap.getLength(); i++){
							Node attrib = attribMap.item(i);
							if(attrib.getNodeName().compareToIgnoreCase(currentName) == 0){
								currentNode = attrib;
							}
						}
					}
				}
			}
			lastName= currentName;
		}
		if(resultIsAttribute) return currentNode.getNodeValue();
		else return currentNode.getTextContent();
	}

	public static void main(String[] argv) throws ParserConfigurationException, IOException, SAXException {
		String f_test = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><resqml22:HorizonInterpretation xmlns:resqml22=\"http://www.energistics.org/energyml/data/resqmlv2\" xmlns:eml23=\"http://www.energistics.org/energyml/data/commonv2\" " +
				"uuid=\"47c542b3-8ee3-4186-ae73-435205677a15\" schemaVersion=\"Resqml 2.2\"><eml23:Citation><eml23:Title>Reverse_WithErosion</eml23:Title><eml23:Originator>Villarubias</eml23:Originator><eml23:Creation>2021-03-11T16:37:19Z</eml23:Creation>" +
				"<eml23:Format>Paradigm SKUA-GOCAD 21 Build:20210511-0137 (id: 2021|1350878|P420211350878|20210511-0137) for Win_x64_10_v16</eml23:Format><eml23:Editor>Villarubias</eml23:Editor><eml23:LastUpdate>2021-03-12T09:46:00Z</eml23:LastUpdate>" +
				"</eml23:Citation><eml23:OSDUIntegration><eml23:OwnerGroup>group1@geosiris.com</eml23:OwnerGroup><eml23:OwnerGroup>group2@geosiris.com</eml23:OwnerGroup><eml23:ViewerGroup>group3@geosiris.com</eml23:ViewerGroup>" +
				"<eml23:ViewerGroup>group4@geosiris.com</eml23:ViewerGroup><eml23:LegalTags>Example legaltags 1</eml23:LegalTags><eml23:LegalTags>Example legaltags 2</eml23:LegalTags></eml23:OSDUIntegration><resqml22:Domain>depth</resqml22:Domain>" +
				"<resqml22:InterpretedFeature><eml23:Uuid>987000fe-8958-466d-8362-aa5b245b7c41</eml23:Uuid><eml23:Title>A2</eml23:Title></resqml22:InterpretedFeature><resqml22:IsConformableAbove>true</resqml22:IsConformableAbove>" +
				"<resqml22:IsConformableBelow>false</resqml22:IsConformableBelow></resqml22:HorizonInterpretation>";

		try {
			Document d = db.parse(new ByteArrayInputStream(f_test.getBytes(StandardCharsets.UTF_8)));

//			logger.info(d.getFirstChild().getNodeName());
//			logger.info(d.getFirstChild().getLocalName());
//			logger.info(d.getFirstChild().getNamespaceURI());
			logger.info(getEnergymlTypeInXml(d));
			logger.info(xml_getSubNodecontent(d, "Citation.Title"));
			logger.info(xml_getSubNodecontent(d, "uuid"));
			logger.info(getUriInXml(d, "test"));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
