package info.bitrich.xchangestream.bitfinex.dto;

import java.util.List;
import java.util.Objects;

public class BitfinexWebSocketAuthNotification {
    private List<BitfinexWebSocketAuthOrder> notifyInfo;
    private String text;

    public BitfinexWebSocketAuthNotification(List<BitfinexWebSocketAuthOrder> notifyInfo, String text) {
        this.notifyInfo = notifyInfo;
        this.text = text;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final BitfinexWebSocketAuthNotification that = (BitfinexWebSocketAuthNotification) o;
        return Objects.equals(notifyInfo, that.notifyInfo) &&
                Objects.equals(text, that.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(notifyInfo, text);
    }

    public List<BitfinexWebSocketAuthOrder> getNotifyInfo() {
        return notifyInfo;
    }

    public String getText() {
        return text;
    }
}
