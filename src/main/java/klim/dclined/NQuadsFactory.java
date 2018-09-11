/*
 * Copyright (C) 2018 Michail Klimenkov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package klim.dclined;

/**
 * @author Michail Klimenkov
 */
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
