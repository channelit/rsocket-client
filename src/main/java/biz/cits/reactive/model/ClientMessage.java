package biz.cits.reactive.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
//@JsonSerialize(as = AutoValue.class)
//@JsonDeserialize(builder = ClientMessage.Builder.class)
public class ClientMessage implements Message {

    private Object id;
    private String client;
    private String content;
    private Instant messageDateTime;

//    public void setId(Object id) {
//        this.id = id;
//    }
//
//    public void setClient(String client) {
//        this.client = client;
//    }
//
//    public void setContent(String content) {
//        this.content = content;
//    }
//
//    public void setMessageDateTime(Instant messageDateTime) {
//        this.messageDateTime = messageDateTime;
//    }
//
//    public ClientMessage() {
//    }
//
//    public ClientMessage(Builder builder) {
//        this.id = builder.id;
//        this.client = builder.client;
//        this.content = builder.content;
//        this.messageDateTime = builder.messageDateTime;
//    }
//
//    @Override
//    public Object getId() {
//        return id;
//    }
//
//    @Override
//    public String getClient() {
//        return client;
//    }
//
//    @Override
//    public String getContent() {
//        return content;
//    }
//
//    @Override
//    public Instant getMessageDateTime() {
//        return messageDateTime;
//    }
//
//    //    @AutoValue.Builder
//    @JsonPOJOBuilder
//    public static class Builder {
//
//        private Object id;
//        private String client;
//        private String content;
//        private Instant messageDateTime;
//
//        public Builder setId(Object id) {
//            this.id = id;
//            return this;
//        }
//
//        public Builder setClient(String client) {
//            this.client = client;
//            return this;
//        }
//
//        public Builder setContent(String content) {
//            this.content = content;
//            return this;
//        }
//
//        public Builder setMessageDateTime(Instant messageDateTime) {
//            this.messageDateTime = messageDateTime;
//            return this;
//        }
//
//        public ClientMessage build() {
//            return new ClientMessage(this);
//        }
//
//    }

}
