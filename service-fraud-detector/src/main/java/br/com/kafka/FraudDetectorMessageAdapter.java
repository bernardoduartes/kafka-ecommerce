package br.com.kafka;

import com.google.gson.*;

import java.lang.reflect.Type;

public class FraudDetectorMessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {
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
        var obj = json.getAsJsonObject();
        var currelationId = (CurrelationId) context.deserialize(obj.get("currelationId"), CurrelationId.class);
        var payload = context.deserialize(obj.get("payload"), Order.class);
        return new Message(currelationId, payload);
    }
}
