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
import Energistics.Etp.v12.Datatypes.Object.DataObject;
import Energistics.Etp.v12.Datatypes.Uuid;
import Energistics.Etp.v12.Protocol.Core.ProtocolException;
import com.geosiris.etp.ETPError;
import com.geosiris.etp.utils.ETPUtils;
import com.google.common.primitives.Chars;
import com.google.gson.*;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.specific.SpecificRecordBuilderBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
public class Message {
	public static final Logger logger = LogManager.getLogger(Message.class);

	private MessageHeader header;
	private SpecificRecordBase body;
	private MessageEncoding encoding;

	public static byte[] encode(SpecificRecordBase mh) {
		SpecificDatumWriter<SpecificRecordBase> recordWriter = new SpecificDatumWriter<>(mh.getSchema());
		ByteArrayOutputStream bf = new ByteArrayOutputStream();
		BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(bf, null);
		try {
			recordWriter.write(mh, binaryEncoder);
		} catch (IOException e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
		try {
			binaryEncoder.flush();
			bf.flush();
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
//		logger.debug("<> " + bf.toByteArray().length + " == '" + bf.toString()+ "' " + mh);
		return bf.toByteArray();
	}

	public static String encodeJson(Message msg) {
		return encodeJson(msg.header) + encodeJson(msg.body);
	}

	public static String encodeJson(SpecificRecordBase mh) {
		SpecificDatumWriter<SpecificRecordBase> recordWriter = new SpecificDatumWriter<>(mh.getSchema());
		ByteArrayOutputStream bf = new ByteArrayOutputStream();
		try {
			JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(mh.getSchema(), bf);
			recordWriter.write(mh, jsonEncoder);
			jsonEncoder.flush();
			bf.flush();
		} catch (IOException ignore) {
		}
		return bf.toString();
	}

	public static class InterfaceSerializer<T>
			implements JsonDeserializer<T> {

		private final Class<T> implementationClass;

		private InterfaceSerializer(final Class<T> implementationClass) {
			this.implementationClass = implementationClass;
		}

		static <T> InterfaceSerializer<T> interfaceSerializer(final Class<T> implementationClass) {
			return new InterfaceSerializer<>(implementationClass);
		}

		@Override
		public T deserialize(JsonElement jsonElement, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
			return context.deserialize(jsonElement, implementationClass);
		}
	}

	public static <T extends SpecificRecordBase> T decodeJson(String data, T obj) {
		Gson gson = new GsonBuilder().registerTypeAdapter(CharSequence.class, new InterfaceSerializer<>(String.class))
				.create();

		return (T) gson.fromJson(data, obj.getClass());
//		return null;
	}

	public static Boolean isFinalePartialMsg(MessageHeader mh) {
		return //mh.getMessageType()!=0 ||
				(mh.getMessageFlags() & MessageFlags.FINALPART) != 0
						|| mh.getCorrelationId() == 0;
	}

	public static Message reassembleChunk(List<Message> multipartMsg){
		Message referencer = null;
		Message result = null;

		//  on rassemble tout dans un seul msg (concat de toutes les map)
		for(Message msg : multipartMsg) {
			if (msg.isChunkMsgReferencer()) {
				if (referencer == null)
					referencer = msg;
				else {

					Object ref_data_objs = null;
					Object msg_data_objs = null;
					try {
						ref_data_objs = ETPUtils.getAttributeValue(referencer.body, "dataObjects");
						msg_data_objs = ETPUtils.getAttributeValue(msg.body, "dataObjects");
					} catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
						logger.error(e.getMessage());
						logger.debug(e.getMessage(), e);
					}
					try {
						ETPUtils.setAttributeValue(referencer.body, "dataObjects", ref_data_objs == null ? msg_data_objs : ETPUtils.concat(ref_data_objs, msg_data_objs));
					} catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
						logger.error(e.getMessage());
						logger.debug(e.getMessage(), e);
					}
				}
			}
		}
		Object doCollection = null;
		try {
			assert referencer != null;
			doCollection = ETPUtils.getAttributeValue(referencer.body, "dataObjects");
		} catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}

		//  todo : faire un sort ?
		if (referencer != null && doCollection !=null){

			Function<DataObject, DataObject> innerOperateDataObject = (DataObject dataObject) ->{
				for (Message _msg : multipartMsg){
					if (_msg.isChunkMsg() && ((DataObject)_msg.body).getBlobId() == dataObject.getBlobId()
					){
						dataObject.getData().put(((DataObject) _msg.body).getData());
//						dataObject.setData( dataObject.getData() == null ? ((DataObject) _msg.body).getData() : dataObject.getData() + ((DataObject) _msg.body).getData());
					}
					// TODO : on ne test pas le "is_final" du dernier chunk mais peut importe
				}
				if (dataObject.getData() != null && dataObject.getData().hasRemaining()){
					dataObject.setBlobId(null);  // pas de blob_id ET data en meme temps
				}
				return dataObject;
			};

			if (doCollection instanceof Collection)
				for (DataObject dataObj : (Collection<DataObject>) doCollection)
					innerOperateDataObject.apply(dataObj);
			else if (doCollection instanceof Map)
				for (DataObject dataObj : ((Map<?, DataObject>) doCollection).values())
					innerOperateDataObject.apply(dataObj);
			else
				logger.error("#etpproto.message@reassemble_chunk : not supported chunckable message data_objects type : " + doCollection.getClass());
			result = referencer;
		}
		return result;
	}


	public Message(Message msg) {
		super();
		this.header = MessageHeader.newBuilder(msg.header).build();
		try {
			Method m_newBuilder = msg.body.getClass().getMethod("newBuilder", msg.body.getClass());
			this.body = (SpecificRecordBase) ((SpecificRecordBuilderBase<?>)m_newBuilder.invoke(msg.body, msg.body)).build();
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
		this.encoding = msg.encoding;
	}

	public Message(byte[] msg) {
		try {
			Decoder dec = DecoderFactory.get().binaryDecoder(msg, null);
			// HEADER
//			this.header = MessageHeader.fromByteBuffer(msg);
			this.header = new MessageHeader();
			SpecificDatumReader<MessageHeader> headerReader = new SpecificDatumReader<>(
					this.header.getSchema(), this.header.getSchema());
			headerReader.read(this.header, dec);

			// BODY
			Class<? extends SpecificRecordBase> etpObjClass = (Class<? extends SpecificRecordBase>) ProtocolsUtility.getEtpClassFromIds(this.header.getProtocol(), this.header.getMessageType());
			logger.debug("OBJ CLASS : " + etpObjClass);
			assert etpObjClass != null;
			this.body = etpObjClass.getConstructor().newInstance();
			ProtocolsUtility.decode(this.body, dec);

			if(this.body == null){
				Decoder dec2 = DecoderFactory.get().binaryDecoder(msg, null);
				headerReader.read(this.header, dec2);
				this.body = new ProtocolException();
				ProtocolsUtility.decode(this.body, dec);

			}
		} catch (Exception e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
	}
	public Message(ByteBuffer msg) {
		this(msg.array());
	}
	public Message(String msg) {
		try {
			// HEADER
			this.header = new MessageHeader();
			JsonDecoder dec = DecoderFactory.get().jsonDecoder(this.header.getSchema(), msg);
			SpecificDatumReader<MessageHeader> headerReader = new SpecificDatumReader<>(this.header.getSchema(), this.header.getSchema());
			headerReader.read(this.header, dec);

			// BODY
			Class<?> etpObjClass = ProtocolsUtility.getEtpClassFromIds(this.header.getProtocol(), this.header.getMessageType());
			assert etpObjClass != null;
			this.body = (SpecificRecordBase) etpObjClass.getConstructor().newInstance();
			JsonDecoder jsonDec = DecoderFactory.get().jsonDecoder(this.body.getSchema(), msg.substring(msg.indexOf(";") + 1));
			ProtocolsUtility.decode(this.body, jsonDec);
		} catch (Exception e) { logger.error(e.getMessage()); logger.debug(e.getMessage(), e); }
	}
	public Message(MessageEncoding encoding, MessageHeader header, SpecificRecordBase body) {
		super();
		this.header = header;
		this.body = body;
		this.encoding = encoding;
	}

	public Message(MessageHeader header, SpecificRecordBase body) {
		this(MessageEncoding.BINARY, header, body);
	}

	public Message(SpecificRecordBase body, long msgId, long correlationId, int msgFlags) {
		this(MessageEncoding.BINARY, body, msgId, correlationId, msgFlags);
	}

	public Message(MessageEncoding encoding, SpecificRecordBase body, long msgId, int msgFlags) {
		this(encoding, body, msgId, 0, msgFlags);
	}
	public Message(MessageEncoding encoding, SpecificRecordBase body, long msgId) {
		this(encoding, body, msgId, 0, MessageFlags.NONE);
	}
	public Message(MessageEncoding encoding, SpecificRecordBase body, long msgId, long correlationId, int msgFlags) {

		this.header = new MessageHeader();
		this.header.setMessageFlags(msgFlags);
//		logger.debug("##// Msg flag : " + this.header.getMessageFlags());
		//logger.debug("Creating etp message for class " + body.getClass() + " ---> " + ETPinfo.getProp(body.getClass(), "messageType"));
		this.header.setProtocol(Integer.parseInt(Objects.requireNonNull(ETPinfo.getProp(body.getClass(), "protocol"))));
		this.header.setMessageType(Integer.parseInt(Objects.requireNonNull(ETPinfo.getProp(body.getClass(), "messageType"))));
		this.header.setCorrelationId(correlationId);
//		int flag = 1 << 2;
		this.header.setMessageId(msgId);
		this.body = body;

		this.encoding = encoding;

	}

	public List<byte[]> encodeMessage(int max_bytes_per_msg, ETPConnection connection) {
		List<byte[]> msgResult = new ArrayList<>();

		boolean is_a_request = !this.body.getClass().getSimpleName().toLowerCase().endsWith("response");

		// Header encoding
		byte[] out_h0 = encode(this.header);

		// Body encoding
		byte[] out_body = encode(this.body);

		// Size computation
		int headerSize = out_h0.length;
		int bodySize = out_body.length;

		if (max_bytes_per_msg > 0 && headerSize + bodySize > max_bytes_per_msg){
			// logger.debug("Size exceed max, try to split");
			// Message exceed the max_bytes_per_msg
			if (this.isPluralMsg()) {
				Message msg1 = new Message(this);
				// get the first dict attribute
				// split it in x factor (as the message is x times too large)
				String dict_attrib_name = ETPUtils.getFirstAttributeMatchingType_name(msg1.body, Map.class);
				Map<CharSequence, Object> dict_value = (Map<CharSequence, Object>) ETPUtils.getFirstAttributeMatchingType_value(msg1.body, Map.class);

				if (dict_value.size() > 1) {
					Message msg2 = new Message(this);
					msg2.header.setMessageId(connection.consumeMessageId());

					// si msg1 etait un vrai premier message on met le suivant au bon correlation id
					if (msg1.header.getCorrelationId() == 0)
						msg2.header.setCorrelationId(msg1.header.getMessageId());

					if (is_a_request) {
						// si requete on met le correlation_id sur le l'id du premier message
						// si this a un correlation_id alors c'est que ce n'était pas le premier
						msg2.header.setCorrelationId(
								this.header.getCorrelationId() != 0 ?
										this.header.getCorrelationId() : this.header.getMessageId()
						);
					}
					// else : // rien a changer le correlation_id est deja sur le meme que this car c'est celui de la requete recu

					Map<CharSequence, Object> d0 = new HashMap<>();
					Map<CharSequence, Object> d1 = new HashMap<>();
					// splitting dict in 2 parts
					for (Map.Entry<CharSequence, Object> entry : dict_value.entrySet() ){
						if (d0.size() < dict_value.size() / 2)
							d0.put(entry.getKey(), entry.getValue());
						else
							d1.put(entry.getKey(), entry.getValue());
					}
					try{
						ETPUtils.setAttributeValue(msg1.body, dict_attrib_name, d0);
						ETPUtils.setAttributeValue(msg2.body, dict_attrib_name, d1);
					}catch (Exception e){e.printStackTrace();}

					msg1.addHeaderFlag(MessageFlags.MULTIPART);
					msg2.addHeaderFlag(MessageFlags.MULTIPART);

					msg1.setFinalMsg(false);
					msgResult.addAll(msg1.encodeMessage(max_bytes_per_msg, connection));
					msgResult.addAll(msg2.encodeMessage(max_bytes_per_msg, connection));
				} else {
					try {
						msgResult.addAll(_encodeMessageChunk(this, bodySize, max_bytes_per_msg, connection));
					} catch (ETPError.InternalError e) {
						logger.error(e.getMessage());
						logger.debug(e.getMessage(), e);
						Message msg_err = e.to_etp_message(connection.consumeMessageId(),
								header.getCorrelationId() != 0 ? this.header.getCorrelationId() : 0);
						msg_err.setFinalMsg(true);
						msgResult.addAll(msg_err.encodeMessage(-1, connection));
					}
				}
			}else{ // erreur
				logger.error("not a plural Message, can not be sent " + this.header);
				Message msg_err = new ETPError.MaxSizeExceededError().to_etp_message(connection.consumeMessageId(),
						header.getCorrelationId() != 0 ? this.header.getCorrelationId() : 0);
				msg_err.setFinalMsg(true);
				msgResult.addAll(msg_err.encodeMessage(-1, connection));
			}
		}else{  // Original Message doesn't exceed max_bytes_per_msg
			// this.set_final_msg()
//			logger.debug("FIN : " + this.header);
			msgResult.add(ETPUtils.concatWithArrayCopy(out_h0, out_body));
		}
		return msgResult;
	}

	/**
	 * Gives the message Id or the correlation depending if the message is related to an other one or not.
	 * @return
	 */
	public long getReferenceId() {
		if(header.getCorrelationId()==0) {
			return header.getMessageId();
		}
		return header.getCorrelationId();
	}

	public MessageHeader getHeader() {
		return header;
	}

	public void setHeader(MessageHeader header) {
		this.header = header;
	}


	public SpecificRecordBase getBody() {
		return body;
	}


	public void setBody(SpecificRecordBase body) {
		this.body = body;
	}

	public Boolean isFinalePartialMsg() {
		return isFinalePartialMsg(this.header);
	}

	////////////////////////////////

	public boolean isFinalMsg() {
		return (this.header.getMessageFlags() & MessageFlags.FINALPART) != 0;
	}

	public boolean isMultipartMsg() {
		return (this.header.getMessageFlags() & MessageFlags.MULTIPART) != 0;
	}

	public boolean isMsgBodyCompressed() {
		return (this.header.getMessageFlags() & MessageFlags.COMPRESSED) != 0;
	}

	public boolean isAskingAcknowledge() {
		return (this.header.getMessageFlags() & MessageFlags.ACKNOWLEDGE) != 0;
	}

	public boolean isIncludingOptionalExtension() {
		return (this.header.getMessageFlags() & MessageFlags.HAS_HEADER_EXTENSION) != 0;
	}

	public void setFinalMsg(boolean is_final) {
		if (is_final)
			this.addHeaderFlag(MessageFlags.FINALPART);
		else
			this.removeHeaderFlag(MessageFlags.FINALPART);
	}

	public void addHeaderFlag(int msg_flag) {
		this.header.setMessageFlags(this.header.getMessageFlags() | msg_flag);
	}

	public void removeHeaderFlag(int msg_flag) {
		this.header.setMessageFlags(this.header.getMessageFlags() & ~(msg_flag));
	}

	public boolean isPluralMsg() {
		return this.body != null && this.body.getClass().getSimpleName().matches(".*s(Response)?$");
	}

	public boolean isChunkMsg() {
		return this.body.getClass().getSimpleName().compareToIgnoreCase("chunk") == 0;
	}

	/**
	 * Returns true if the message is a "chunkable" one and doesn't contain any data, but a blobId.
	 * This means that data will be sent after in chunk messages
	 * @return
	 */
	public boolean isChunkMsgReferencer() {
		try {
			Object data_objects = ETPUtils.getAttributeValue(this.body, "DataObjects");

			if (!this.isChunkMsg() && data_objects != null) {
				if (data_objects instanceof List) {
					List<DataObject> data_objects_list = (List<DataObject>) data_objects;
					for (DataObject dataObj : data_objects_list) {
						if (!(dataObj.getBlobId() != null
								&& (dataObj.getData() == null || dataObj.getData().array().length == 0)
						))
							return false;
					}
					return true;
				} else {
					Map<String, DataObject> data_objects_map = (Map<String, DataObject>) data_objects;
					for (DataObject dataObj : data_objects_map.values()) {
						if (!(dataObj.getBlobId() != null
								&& (dataObj.getData() == null || dataObj.getData().array().length == 0)
						))
							return false;
					}
					return true;
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
		return false;
	}

	public List<byte[]> _encodeMessageChunk(
			Message chunkable_msg,
			int encoded_msg_size,
			int max_bytes_per_msg,
			ETPConnection connection
	) throws ETPError.InternalError {
		List<byte[]> result;

		int secure_size = 50;  // TODO : ameliorer pour que le chunk fasse vraiment la taille max d'un message (il faudrait connaitre la taille de ce qui n'est pas binaire dans le chunk message)
		int size_of_chunks = max_bytes_per_msg - secure_size; // substract 50 for header part (header takes 5?) and non binary part of the chunk message

		List<String> do_names = Arrays.asList("dataObjects", "dataArrays");

		Object data_objs = null;
		try {
			for(String do_name: do_names) {
				data_objs = ETPUtils.getAttributeValue(chunkable_msg.body, do_name);
				if(data_objs != null)
					break;
			}
		} catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
			logger.error(e.getMessage());
			logger.debug(e.getMessage(), e);
		}
		boolean msg_was_final = chunkable_msg.isFinalMsg();

		long correlation_id = chunkable_msg.header.getCorrelationId() != 0 ?
				chunkable_msg.header.getCorrelationId()
				: chunkable_msg.header.getMessageId();


		// else : // rien a changer le correlation_id est deja sur le meme que self car c'est celui de la requete recu

		if (data_objs != null){  // si on a une list/Map de dataObjects
			// get the chunks class
			Class<?> chunk_class = ProtocolsUtility.getEtpClassFromProtocolIdAndName(
					chunkable_msg.header.getProtocol(),
					"chunk"
			);

			if (chunk_class != null){

				List<SpecificRecordBase> lst_chunks = new ArrayList<>();  // all chunks of all dataObjects

				// for blob_id see 3.7.3.2 of the documentation :
				// blob_id is assign to one entire DataObject and is refered in all chunck of this dataObject

				if (data_objs instanceof Map){
					Map<?, ?> dataObjsMap = (Map<?,?>) data_objs;
					for(Object dataObj_o : dataObjsMap.values()) {
						DataObject dataObj = (DataObject) dataObj_o;
						ByteBuffer data = dataObj.getData();
						Uuid blob_id = new Uuid(ETPUtils.asBytes(UUID.randomUUID()));

						while (data.hasRemaining()) {
							final int ci_size = Math.min(size_of_chunks, data.remaining());
							byte[] chk = new byte[ci_size];
							data.get(chk, 0, ci_size);
							SpecificRecordBase chunkMsg = null;
							try {
								chunkMsg = (SpecificRecordBase) chunk_class.getDeclaredConstructor(Uuid.class, ByteBuffer.class, Boolean.class).newInstance(blob_id, ByteBuffer.wrap(chk), false);
								lst_chunks.add(chunkMsg);
							} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
								logger.error(e.getMessage());
								logger.debug(e.getMessage(), e);
							}
						}
						if (lst_chunks.size() > 0) {
							try {
								ETPUtils.setAttributeValue(lst_chunks.get(lst_chunks.size() - 1), "final", true);
							} catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
								logger.error(e.getMessage());
								logger.debug(e.getMessage(), e);
							}
						}
						try {
							ETPUtils.setAttributeValue(dataObj, "blobId", blob_id);
							ETPUtils.setAttributeValue(dataObj, "data", ByteBuffer.wrap(new byte[0]));// removing the data from the message when blob_id is populated
						} catch (Exception e) {
							logger.error(e.getMessage());
							logger.debug(e.getMessage(), e);
						}
					}
				}else if (data_objs instanceof List){
					List<?> dataObjs_list = (List<?>) data_objs;
					for(Object dataObj_o : dataObjs_list) {
						DataObject dataObj = (DataObject) dataObj_o;
						ByteBuffer data = dataObj.getData();
						Uuid blob_id = new Uuid(ETPUtils.asBytes(UUID.randomUUID()));

						while (data.hasRemaining()) {
							final int ci_size = Math.min(size_of_chunks, data.remaining() - data.position());
							byte[] chk = new byte[ci_size];
							data.get(chk, 0, ci_size);
							SpecificRecordBase chunkMsg = null;
							try {
								chunkMsg = (SpecificRecordBase) chunk_class.getDeclaredConstructor(Uuid.class, ByteBuffer.class, Boolean.class).newInstance(blob_id, ByteBuffer.wrap(chk), false);
								lst_chunks.add(chunkMsg);
							} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
								logger.error(e.getMessage());
								logger.debug(e.getMessage(), e);
							}
						}
						if (lst_chunks.size() > 0) {
							try {
								ETPUtils.setAttributeValue(lst_chunks.get(lst_chunks.size() - 1), "final", true);
							} catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
								logger.error(e.getMessage());
								logger.debug(e.getMessage(), e);
							}
						}
						try {
							ETPUtils.setAttributeValue(dataObj, "blobId", blob_id);
							ETPUtils.setAttributeValue(dataObj, "data", ByteBuffer.wrap(new byte[0]));// removing the data from the message when blob_id is populated
						} catch (Exception e) {
							logger.error(e.getMessage());
							logger.debug(e.getMessage(), e);
						}
					}
				}

				// chunkable_msg.data_objects = data_objs  // useless ?
				// send the message
				chunkable_msg.setFinalMsg(false);
				chunkable_msg.addHeaderFlag(MessageFlags.MULTIPART);
				result = new ArrayList<>(chunkable_msg.encodeMessage(max_bytes_per_msg, connection));

				// send chunks
				for(SpecificRecordBase chunk : lst_chunks.subList(0, lst_chunks.size()-1))
					result.addAll(new Message(chunk, connection.consumeMessageId(), correlation_id, MessageFlags.MULTIPART).encodeMessage(max_bytes_per_msg, connection));

				result.addAll(new Message(lst_chunks.get(lst_chunks.size()-1), connection.consumeMessageId(), correlation_id, msg_was_final ? MessageFlags.MULTIPART_AND_FINALPART : MessageFlags.MULTIPART).encodeMessage(max_bytes_per_msg, connection));

				// TODO : potentielle erreur de MultipartCancelledError si le nb de reponse depasse le nombre max

			}else{
				throw new InternalError ("@Message : No chunck class found for protocol " + chunkable_msg.header.getProtocol());
			}
		}else {
			throw new ETPError.InternalError("@Message : No data_object found " + chunkable_msg.header.getProtocol() + " for msg "
					+ chunkable_msg.body.getClass());
		}
		// raise une erreur ?
		return result;
	}

}
