package biz.cits.reactive.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ClientMessage implements Message {

    private Object id;
    private String client;
    private String content;
    private Instant messageDateTime;

}
