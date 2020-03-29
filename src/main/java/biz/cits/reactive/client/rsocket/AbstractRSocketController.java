package biz.cits.reactive.client.rsocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.lang.reflect.ParameterizedType;

public abstract class AbstractRSocketController<T> {

    private final Class<T> tClass;

    private final RSocketRequester rSocketRequester;

    private ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule()).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);


    public AbstractRSocketController(T t, RSocketRequester rSocketRequester) {
        this.tClass = (Class<T>) t.getClass();
        this.rSocketRequester = rSocketRequester;
    }

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
