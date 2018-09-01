package klim.dclined;

import org.junit.jupiter.api.Test;

import static klim.dclined.NQuadsFactory.nQuad;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NQuadsTest {

    @Test
    public void testBuilder() {
        String nquads = nQuad("_:starm", "email", "starmaker@mail.com")
                .nQuad("<0x242>", "friend", "<0x243>")
                .nQuad("<0x345>", "*", "*").toString();

        String expected = "_:starm <email> \"starmaker@mail.com\" .\n" +
                "<0x242> <friend> <0x243> .\n" +
                "<0x345> * * .\n";

        assertEquals(expected, nquads);
    }
}
