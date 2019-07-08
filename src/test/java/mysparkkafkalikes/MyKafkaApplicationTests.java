package mysparkkafkalikes;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class MyKafkaApplicationTests {

    private static final CountDownLatch latch = new CountDownLatch(1);

    @Rule
    public OutputCapture outputCapture = new OutputCapture();

    @Test
    public void testVanillaExchange() throws Exception {
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(this.outputCapture.toString().contains("A simple test message"))
                .isTrue();
    }
}
