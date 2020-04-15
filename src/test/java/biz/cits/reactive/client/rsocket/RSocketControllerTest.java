package biz.cits.reactive.client.rsocket;

import io.rsocket.RSocket;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@RunWith(SpringRunner.class)
@WebFluxTest(value = RSocketController.class)
public class RSocketControllerTest {

    @Autowired
    private WebTestClient testClient;

    @MockBean
    private RSocketRequester rSocketRequester;

    @MockBean
    private RSocket rSocket;

    @Mock
    private RSocketRequester.RequestSpec requestSpec;


    @Test
    public void whenRequest_ThenCheckResponse() throws Exception {
        when(rSocketRequester.route("messages/me")).thenReturn(requestSpec);
        when(requestSpec.data(any())).thenReturn(requestSpec);
        String data = "test";
        when(requestSpec.retrieveFlux(String.class)).thenReturn(Flux.just("test"));

        FluxExchangeResult<String> result = testClient.get()
                .uri("/messages/me")
                .exchange()
                .expectStatus()
                .isOk()
                .returnResult(String.class);

        Flux<String> resultFlux = result.getResponseBody();
        StepVerifier.create(resultFlux)
                .expectNext(data)
                .thenCancel()
                .verify();

    }
}
