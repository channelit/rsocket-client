package biz.cits.reactive.client.rsocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public abstract class AbstractRSocketController<T> {
    Class<T> tClass;

    private final RSocketRequester rSocketRequester;

    ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());


    public AbstractRSocketController(RSocketRequester rSocketRequester) {
        this.rSocketRequester = rSocketRequester;
    }

    @GetMapping(value = "/typed-messages/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<T> getMessages(@PathVariable String filter) {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        return rSocketRequester
                .route("messages/" + filter)
                .data(filter)
                .retrieveFlux(String.class).map(this::toT);
    }

    private T toT(String json) {
        try {
            return mapper.readValue(json, tClass);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Invalid JSON: " + json, e);
        }
    }

}
