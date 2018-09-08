package klim.dclined;

public class NQuadsFactory {

    /**
     * Creates new instance of NQuads and appends the given nquad to it.
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
     * @return created instance of NQuads
     */
    public static NQuads nQuad(String subject, String predicate, String object) {
        NQuads builder = new NQuads();
        builder.nQuad(subject, predicate, object);
        return builder;
    }

    public static String uid(String uid) {
        return "<" + uid + ">";
    }

}
