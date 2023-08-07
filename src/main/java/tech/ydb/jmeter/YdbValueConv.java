package tech.ydb.jmeter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.math.BigDecimal;
import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * Text-to-value YDB data type convertors.
 * @author zinal
 */
public class YdbValueConv {

    public static Value<?> convert(String type, String value) {
        boolean optional = false;
        if (type.endsWith("?")) {
            optional = true;
            type = type.substring(0, type.length()-1);
        }
        Conv conv = HANDLERS.get(type.toLowerCase());
        if (conv==null) {
            throw new UnsupportedOperationException("Unsupported YDB data type: " + type);
        }
        return conv.convert(value, optional);
    }

    public static String convert(Value<?> value) {
        if (value==null) {
            return null;
        }
        switch (value.getType().getKind()) {
            case OPTIONAL:
                if (! value.asOptional().isPresent())
                    return null;
                value = value.asOptional().get();
        }
        switch (value.getType().getKind()) {
            case PRIMITIVE:
                PrimitiveValue pv = value.asData();
                switch (pv.getType()) {
                    case Text:
                        return pv.getText();
                    case Json:
                        return pv.getJson();
                    case JsonDocument:
                        return pv.getJsonDocument();
                    case Bool:
                        return String.valueOf(pv.getBool());
                    case Int8:
                        return String.valueOf(pv.getInt8());
                    case Int16:
                        return String.valueOf(pv.getInt16());
                    case Int32:
                        return String.valueOf(pv.getInt32());
                    case Int64:
                        return String.valueOf(pv.getInt64());
                    case Uint8:
                        return String.valueOf(pv.getUint8());
                    case Uint16:
                        return String.valueOf(pv.getUint16());
                    case Uint32:
                        return String.valueOf(pv.getUint32());
                    case Uint64:
                        return String.valueOf(pv.getUint64());
                    case Float:
                        return String.valueOf(pv.getFloat());
                    case Double:
                        return String.valueOf(pv.getDouble());
                    case Date:
                        return pv.getDate().toString();
                    case Datetime:
                        return pv.getDatetime().toString();
                    case Timestamp:
                        return pv.getTimestamp().toString();
                }
                break;
            case DECIMAL:
                return ((DecimalValue) value).toBigDecimal().toPlainString();
        }
        return value.toString();
    }

    private static RuntimeException makeIllegalEmpty() {
        return new IllegalArgumentException("Empty value for non-optional parameter");
    }
    
    private static final HashMap<String, Conv> HANDLERS = new HashMap<>();
    static {
        reg(new ConvBool());
        reg(new ConvInt32());
        reg(new ConvUint32());
        reg(new ConvInt64());
        reg(new ConvUint64());
        reg(new ConvFloat());
        reg(new ConvDouble());
        reg(new ConvDate());
        reg(new ConvDateTime());
        reg(new ConvTimestamp());
        reg(new ConvText());
        reg(new ConvBytes());
        reg(new ConvBase64());
        reg(new ConvDecimal());
    }

    private static void reg(Conv conv) {
        HANDLERS.put(conv.name().toLowerCase(), conv);
    }

    private static interface Conv {
        String name();
        Value<?> convert(String value, boolean optional);
    }

    private static abstract class ConvNum implements Conv {
        private final String name;
        private final OptionalType otype;

        protected ConvNum(String name, Type type) {
            this.name = name;
            this.otype = type.makeOptional();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Value<?> convert(String value, boolean optional) {
            if (value!=null) {
                value = value.trim();
            }
            if (value==null || value.length()==0) {
                if (optional) {
                    return otype.emptyValue();
                }
                throw makeIllegalEmpty();
            }
            Value<?> v = convertNum(value);
            if (optional) {
                v = v.makeOptional();
            }
            return v;
        }
        protected abstract Value<?> convertNum(String value);
    }

    private static class ConvBool extends ConvNum {
        public ConvBool() {
            super("Bool", PrimitiveType.Bool);
        }
        @Override
        public Value<?> convertNum(String value) {
            if (value.equalsIgnoreCase("true")
                    || value.equalsIgnoreCase("yes")
                    || value.equalsIgnoreCase("t")
                    || value.equalsIgnoreCase("1"))
                return PrimitiveValue.newBool(true);
            if (value.equalsIgnoreCase("false")
                    || value.equalsIgnoreCase("no")
                    || value.equalsIgnoreCase("f")
                    || value.equalsIgnoreCase("0"))
                return PrimitiveValue.newBool(true);
            throw new IllegalArgumentException("Illegal value for boolean: " + value);
        }
    }

    private static class ConvInt32 extends ConvNum {
        public ConvInt32() {
            super("Int32", PrimitiveType.Int32);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newInt32(Integer.parseInt(value));
        }
    }

    private static class ConvUint32 extends ConvNum {
        public ConvUint32() {
            super("Uint32", PrimitiveType.Uint32);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newUint32(Long.parseLong(value));
        }
    }

    private static class ConvInt64 extends ConvNum {
        public ConvInt64() {
            super("Int64", PrimitiveType.Int64);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newInt64(Long.parseLong(value));
        }
    }

    private static class ConvUint64 extends ConvNum {
        public ConvUint64() {
            super("Uint64", PrimitiveType.Uint64);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newUint64(Long.parseLong(value));
        }
    }

    private static class ConvFloat extends ConvNum {
        public ConvFloat() {
            super("Float", PrimitiveType.Float);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newFloat(Float.parseFloat(value));
        }
    }

    private static class ConvDouble extends ConvNum {
        public ConvDouble() {
            super("Double", PrimitiveType.Double);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newDouble(Double.parseDouble(value));
        }
    }

    private static class ConvDecimal extends ConvNum {
        public ConvDecimal() {
            super("Decimal", DecimalType.getDefault());
        }
        @Override
        public Value<?> convertNum(String value) {
            return DecimalType.getDefault().newValue(new BigDecimal(value));
        }
    }

    private static class ConvDate extends ConvNum {
        public ConvDate() {
            super("Date", PrimitiveType.Date);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newDate(LocalDate.parse(value));
        }
    }

    private static class ConvDateTime extends ConvNum {
        public ConvDateTime() {
            super("DateTime", PrimitiveType.Datetime);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newDatetime(LocalDateTime.parse(value));
        }
    }

    private static class ConvTimestamp extends ConvNum {
        public ConvTimestamp() {
            super("Timestamp", PrimitiveType.Timestamp);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newTimestamp(Instant.parse(value));
        }
    }

    private static abstract class ConvTextBase implements Conv {
        private final String name;
        private final OptionalType otype;

        public ConvTextBase(String name, Type type) {
            this.name = name;
            this.otype = type.makeOptional();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Value<?> convert(String value, boolean optional) {
            if (value==null) {
                if (optional) {
                    return otype.emptyValue();
                }
                throw makeIllegalEmpty();
            }
            if (value.length()==0) {
                if (optional) {
                    return otype.emptyValue();
                }
            }
            Value<?> v = textConv(value);
            if (optional) {
                v = v.makeOptional();
            }
            return v;
        }

        protected abstract Value<?> textConv(String value);
    }

    private static class ConvText extends ConvTextBase {
        public ConvText() {
            super("Text", PrimitiveType.Text);
        }

        @Override
        protected Value<?> textConv(String value) {
            return PrimitiveValue.newText(value);
        }
    }

    private static class ConvBytes extends ConvTextBase {
        public ConvBytes() {
            super("Bytes", PrimitiveType.Bytes);
        }

        @Override
        protected Value<?> textConv(String value) {
            return PrimitiveValue.newBytes(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class ConvBase64 extends ConvTextBase {
        public ConvBase64() {
            super("Base64", PrimitiveType.Bytes);
        }

        @Override
        protected Value<?> textConv(String value) {
            return PrimitiveValue.newBytes(java.util.Base64.getUrlDecoder().decode(value));
        }
    }

}
