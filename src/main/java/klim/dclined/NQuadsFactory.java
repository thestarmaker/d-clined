package klim.dclined;

public class NQuadsFactory {

    public static NQuads nQuad(String subject, String predicate, String object) {
        NQuads builder = new NQuads();
        builder.nQuad(subject, predicate, object);
        return builder;
    }

    public static String uid(String uid) {
        return "<" + uid + ">";
    }

    public static class NQuads {

        private final StringBuilder builder = new StringBuilder();

        public NQuads nQuad(String subject, String predicate, String object) {
            if (subject.startsWith("_:") || subject.startsWith("<") || subject.startsWith("*")) {
                builder.append(subject);
            } else {
                builder.append("<").append(subject).append(">");
            }

            if (predicate.startsWith("*")) {
                builder.append(" ").append(predicate).append(" ");
            } else {
                builder.append(" ").append("<").append(predicate).append(">").append(" ");
            }

            if (object.startsWith("_:") || object.startsWith("<") || object.startsWith("*")) {
                builder.append(object).append(" .\n");
            } else {
                builder.append("\"").append(object).append("\"").append(" .\n");
            }

            return this;
        }


        @Override
        public String toString() {
            return builder.toString();
        }
    }
}
