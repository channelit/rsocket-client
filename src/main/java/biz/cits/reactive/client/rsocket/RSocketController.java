package biz.cits.reactive.client.rsocket;

import autovalue.shaded.com.google$.common.collect.$TreeRangeSet;
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
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.Resume;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RestController
@Order(1)
public class RSocketController {

    private Mono<RSocket> rSocket;
    private final RSocketRequester rSocketRequester;
    private ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule()).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    Logger logger = LoggerFactory.getLogger(RSocketController.class);
    private Scheduler s;

    public RSocketController(RSocket rSocket, RSocketRequester rSocketRequester) throws InterruptedException {
        s = Schedulers.newSingle("inspect");
        s.schedulePeriodically(this::inspect, 0, 5, TimeUnit.SECONDS);
        s.start();
        init();
        this.rSocketRequester = rSocketRequester;
//        String filter = "ABCDE";
//        String data = "select message FROM messages WHERE (message->>'messageDateTime')::timestamp with time zone > '2020-04-27 09:19:58.89'::timestamp without time zone";
//        long i = 0;
//        RSocket mySocket = this.rSocket.block();
//        while (i < 16000) {
//            i++;
//            data = "{\"id\":\"" + UUID.randomUUID().toString() + "\"}";
//            ObjectNode message = mapper.createObjectNode();
//            message.put("route", "post");
//            message.put("client", "me");
//            message.put("filter", filter);
//            message.put("data", data);
//            System.out.println(i);
//            mySocket.requestStream(DefaultPayload.create(message.toString()).touch()).retry(5).subscribe();
//        }
    }

    private void inspect() {
        ObjectNode message = mapper.createObjectNode();
        message.put("route", "inspect");
        message.put("client", "me");
        message.put("filter", "");
        message.put("data", "");
        Flux<Payload> s = rSocket.flatMapMany(requester ->
                requester.requestStream(DefaultPayload.create(message.toString())));
        s.map(this::logThread).map(Payload::getDataUtf8).subscribe(msg-> logger.info(msg));
    }

    private void init() {
        Resume resume =
                new Resume()
                        .sessionDuration(Duration.ofSeconds(1000))
                        .storeFactory(t -> new InMemoryResumableFramesStore("client", 500_000_000))
                        .cleanupStoreOnKeepAlive()
                        .retry(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1))
                                .doBeforeRetry(
                                        retrySignal -> {
                                            System.out.println("Disconnected. Trying to resume connection...");
                                        })
                                .doAfterRetry(
                                        retrySignal -> {
                                            System.out.println("Tried to resume connection...");
                                        })
                        );

        logger.info("Reconnecting");
        System.out.println("Reconnecting on Thread --------->      " + Thread.currentThread().getName());
        this.rSocket =
                RSocketConnector.create()
                        .reconnect(Retry.fixedDelay(100, Duration.ofSeconds(5)))
                        .resume(resume)
                        .keepAlive(Duration.ofSeconds(1),Duration.ofHours(100))
                        .payloadDecoder(PayloadDecoder.ZERO_COPY)
                        .dataMimeType(WellKnownMimeType.APPLICATION_CBOR.toString())
                        .metadataMimeType(WellKnownMimeType.APPLICATION_CBOR.getString())
                        .connect(TcpClientTransport.create("localhost", 7000));
    }

    @GetMapping(value = "/socket/{route}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> socket(@PathVariable String route) {
        String filter = "ABCDE";
        String data = "select message FROM messages WHERE (message->>'messageDateTime')::timestamp with time zone > '2020-04-27 09:19:58.89'::timestamp without time zone";
        data = "{\"id\":'id'}";
        ObjectNode message = mapper.createObjectNode();
        message.put("route", route);
        message.put("client", "me");
        message.put("filter", filter);
        message.put("data", data);
        Flux<Payload> s = rSocket.flatMapMany(requester ->
                requester.requestStream(DefaultPayload.create(message.toString()))
        ).doOnError(this::handleConnectionError).retry();
        return s.map(this::logThread).map(Payload::getDataUtf8);
    }

    @GetMapping(value = "/socket/bulk-post", produces = MediaType.TEXT_PLAIN_VALUE)
    public Publisher<String> socketBulkPost() {
        String filter = "ABCDE";
        String data = "select message FROM messages WHERE (message->>'messageDateTime')::timestamp with time zone > '2020-04-27 09:19:58.89'::timestamp without time zone";
        long i = 0;
        while (i < 3000) {
            i++;
            data = "{\"id\":\"" + UUID.randomUUID().toString() + "\"}";
            ObjectNode message = mapper.createObjectNode();
            message.put("route", "post");
            message.put("client", "me");
            message.put("filter", filter);
            message.put("data", data);

            System.out.println(i);

            Flux<Payload> s = rSocket.flatMapMany(requester ->
                    requester.requestStream(DefaultPayload.create(message.toString()))
            ).doOnError(this::handleConnectionError).retry();

            s.subscribe();
//            rSocketPublisher.requestStream(DefaultPayload.create(message.toString())).doOnError(this::handleConnectionError).retry().subscribe();
        }
        return Flux.just("ok");
    }

    private Payload logThread(Payload payload) {
        System.out.println("Message received on Thread --------->      " + Thread.currentThread().getName());
        return payload;
    }

    private synchronized void handleConnectionError(Throwable throwable) {
        if (throwable instanceof ClosedChannelException) {
            init();
            System.out.println("Closed Channel Exception Occurred In Subscriber");
        }
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

    @GetMapping(value = "/replay/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> replay(@PathVariable String client, @PathVariable String filter) {
        String query = "select message FROM messages WHERE (message->>'messageDateTime')::timestamp with time zone > '2020-04-27 09:19:58.89'::timestamp without time zone";
//        String query = "select message FROM messages";
        return rSocketRequester
                .route("replay/" + client + "/" + filter)
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
