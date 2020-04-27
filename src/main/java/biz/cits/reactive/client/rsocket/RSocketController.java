package biz.cits.reactive.client.rsocket;

import biz.cits.reactive.model.ClientMessage;
import biz.cits.reactive.model.Message;
import biz.cits.reactive.model.MsgGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.exceptions.RejectedResumeException;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.keepalive.KeepAliveHandler;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.resume.PeriodicResumeStrategy;
import io.rsocket.resume.ResumableDuplexConnection;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@Order(1)
public class RSocketController {

    private RSocket rSocket;
    private final RSocketRequester rSocketRequester;
    private ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule()).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public RSocketController(RSocket rSocket, RSocketRequester rSocketRequester) {
        init();
        this.rSocketRequester = rSocketRequester;
    }

    private void init() {
        this.rSocket = RSocketFactory
                .connect()
                .errorConsumer(throwable -> {
                    if (throwable instanceof ClosedChannelException) {
                        init();
                        throwable.printStackTrace();
                    }
                })
                .resume()
                .keepAliveTickPeriod(Duration.ofSeconds(1))
                .resumeSessionDuration(Duration.ofHours(2))
                .resumeStreamTimeout(Duration.ofHours(5))
                .frameDecoder(PayloadDecoder.ZERO_COPY)
                .mimeType(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toString(), WellKnownMimeType.APPLICATION_CBOR.toString())
                .transport(TcpClientTransport.create("localhost", 7000))
                .start()
                .retryBackoff(Integer.MAX_VALUE, Duration.ofSeconds(1))
                .block();
    }

    @GetMapping(value = "/socket/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> socket(@PathVariable String filter) {
        ObjectNode message = mapper.createObjectNode();
        message.put("route", "messages");
        message.put("client", "me");
        message.put("filter", filter);
        message.put("data", filter);
//        System.out.println(rSocket.availability());
//        if (rSocket.availability() < 1.0) {
//            rSocket.dispose();
//        }
        Flux<Payload> s = rSocket.requestStream(DefaultPayload.create(message.toString()));
        return s.map(Payload::getDataUtf8);
    }


    @GetMapping(value = "/messages/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getMessages(@PathVariable String filter) {
        Flux<String> requester = rSocketRequester
                .route("messages/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
        return requester;
    }

    @GetMapping(value = "/camel/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelMessages(@PathVariable String filter) {
        return rSocketRequester
                .route("camel/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-durable/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelDurableMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-durable/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-durable-direct/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelDurableDirectMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-durable-direct/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-virtual/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelVirtualMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-virtual/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-virtual-direct/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelVirtualDirectMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-virtual-direct/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/replay/{client}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> replay(@PathVariable String client) {
        String query = "select message FROM messages WHERE (message->>'messageDateTime')::timestamp with time zone > '2020-04-27 09:19:58.89'::timestamp without time zone";
        return rSocketRequester
                .route("replay/" + client)
                .data(query)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/post", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Mono<String> postMessags() {
        Map.Entry<String, String> message = MsgGenerator.getMessages(1).get(0);
        Message m = ClientMessage.builder()
                .client(message.getKey())
                .id(UUID.randomUUID())
                .content(message.getValue())
                .messageDateTime(Instant.now()).build();
        String jsonString = "";
        try {
            jsonString = mapper.writeValueAsString(m);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return rSocketRequester
                .route("post/me")
                .data(jsonString).retrieveMono(String.class);
    }

    @GetMapping(value = "/posts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> postMessages(@RequestParam int num) {
        Flux<String> messages = Flux.create(messageFluxSink ->
                MsgGenerator.getMessages(num).forEach(message -> {
                    ClientMessage clientMessage = ClientMessage.builder()
                            .client(message.getKey())
                            .id(UUID.randomUUID().toString())
                            .content(message.getValue())
                            .messageDateTime(Instant.now()).build();
                    String jsonString = "";
                    try {
                        jsonString = mapper.writeValueAsString(clientMessage);

                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    messageFluxSink.next(jsonString);
                }));
        return rSocketRequester
                .route("posts/me")
                .data(messages)
                .retrieveFlux(String.class).map(this::checkResult);
    }

    private String checkResult(String m) {
        try {
            JsonNode json = mapper.readTree(m);
            if (json.has("error")) {
                throw new RuntimeException(json.get("error").asText());
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return m;
    }


}
