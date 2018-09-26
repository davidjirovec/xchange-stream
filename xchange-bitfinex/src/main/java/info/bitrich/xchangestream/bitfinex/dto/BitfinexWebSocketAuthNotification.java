package info.bitrich.xchangestream.bitfinex.dto;

import java.util.Objects;

public class BitfinexWebSocketAuthNotification {
    private String text;

    public BitfinexWebSocketAuthNotification(String text) {
        this.text = text;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BitfinexWebSocketAuthNotification that = (BitfinexWebSocketAuthNotification) o;
        return Objects.equals(text, that.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text);
    }

    public String getText() {
        return text;
    }
}
