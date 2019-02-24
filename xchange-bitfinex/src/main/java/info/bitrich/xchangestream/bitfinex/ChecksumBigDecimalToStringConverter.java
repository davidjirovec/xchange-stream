package info.bitrich.xchangestream.bitfinex;


import javax.script.*;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class ChecksumBigDecimalToStringConverter {

    private static final ScriptEngine SCRIPT_ENGINE = new ScriptEngineManager().getEngineByName("nashorn");

    static {
        try {
            ((Compilable) SCRIPT_ENGINE)
                    .compile(
                            new InputStreamReader(
                                    Objects.requireNonNull(
                                            ChecksumBigDecimalToStringConverter.class
                                                    .getClassLoader()
                                                    .getResourceAsStream("format.js")
                                    )
                            )
                    )
                    .eval();
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

    String convert(final List<BigDecimal> bigDecimals) {
        try {
            return (String) ((Invocable) SCRIPT_ENGINE)
                    .invokeFunction(
                            "format",
                            bigDecimals.stream().map(BigDecimal::doubleValue).collect(Collectors.toList())
                    );
        } catch (ScriptException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

}
