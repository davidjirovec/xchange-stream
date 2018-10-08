package info.bitrich.xchangestream.bitfinex;

import java.math.BigDecimal;

public class ChecksumBigDecimalToStringConverter {

    public String convert(final BigDecimal bigDecimal) {
        return bigDecimal.toString()
                .toLowerCase()
                .replace("+", "")
                .replaceFirst("(.+\\.[^0]*)0+(e.*|$)", "$1$2")
                .replaceFirst("\\.e", "e")
                .replaceFirst("\\.$", "")
                .replaceFirst("(.*\\..*[^0])0+$", "$1");
    }

}
