package klim.dclined;

public class NQuads {

    private final StringBuilder builder = new StringBuilder();

    /**
     * Appends new nquad to the list of nquads in this instance.
     * You can either use blank node references or uids as follows:
     * <code>
     *     //appends email to newly created blank node
     *     nQuad("_:person", "person.email", "starmaker@mail.com");
     *
     *     //joins two nodes identified by uids with predicate
     *     import klim.dclined.NQuadsFactory.uid;
     *
     *     nQuad(uid(0x426), "likes", uid(0x875));
     * </code>
     *
     * @param subject
     * @param predicate
     * @param object
     * @return current instance
     */
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
