package br.com.kafka;

import com.google.gson.*;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {
    @Override
    public JsonElement serialize(Message mssage, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", mssage.getPayload().getClass().getName());
        obj.add("payload", context.serialize(mssage.getPayload()));
        obj.add("currelationId", context.serialize(mssage.getId()));
        return obj;
    }

    @Override
    public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        try {
            var obj = json.getAsJsonObject();
            var payloadType = obj.get("type").getAsString();
            var currelationId = (CurrelationId) context.deserialize(obj.get("currelationId"), CurrelationId.class);
            var payload = context.deserialize(obj.get("payload"), Class.forName(payloadType));

            return new Message(currelationId, payload);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }
}
