package info.bitrich.xchangestream.bitfinex.dto;

import org.junit.Test;

import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;

public class BitfinexOrderbookTest {

    @Test
    public void timestampShouldBeInSeconds() {
        final BitfinexOrderbook bitfinexOrderbook = new BitfinexOrderbook();

        // What is the time now... after order books created?
        assertThat("The timestamp should be a value less than now, but was: " + bitfinexOrderbook.getDate(),
                !bitfinexOrderbook.getDate().after(new Date()));
    }
}
