package br.com.kafka;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message> {
    @Override
    public JsonElement serialize(Message mssage, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", mssage.getPayload().getClass().getName());
        obj.add("payload", context.serialize(mssage.getPayload()));
        obj.add("currelationId", context.serialize(mssage.getId()));
        return obj;
    }
}
