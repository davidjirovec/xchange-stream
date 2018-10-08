package info.bitrich.xchangestream.bitfinex;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class ChecksumBigDecimalToStringConverterTest {

    private final ChecksumBigDecimalToStringConverter converter = new ChecksumBigDecimalToStringConverter();

    @Test
    public void test1() {
        Assert.assertEquals("1e7", converter.convert(new BigDecimal("1.0E+7")));
    }

    @Test
    public void test2() {
        Assert.assertEquals("1e7", converter.convert(new BigDecimal("1E+7")));
    }

    @Test
    public void test3() {
        Assert.assertEquals("10", converter.convert(new BigDecimal("10")));
    }

    @Test
    public void test4() {
        Assert.assertEquals("10", converter.convert(new BigDecimal("10.0")));
    }

    @Test
    public void test5() {
        Assert.assertEquals("0.000001", converter.convert(new BigDecimal("0.0000010")));
    }
}
