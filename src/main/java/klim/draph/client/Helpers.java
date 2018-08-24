package klim.draph.client;

import java.util.HashMap;
import java.util.Map;

public class Helpers {
    public static Map<Integer, Long> mergeIdMaps(Map<Integer, Long> src, Map<Integer, Long> dst) {
        Map<Integer, Long> finalDst = new HashMap<>();
        src.entrySet()
                .stream()
                .filter((Map.Entry<Integer, Long> e) -> {
                    Long dstValue = dst.get(e.getKey());
                    return dstValue == null || dstValue < e.getValue();
                }).forEach((Map.Entry<Integer, Long> e) -> {
            finalDst.put(e.getKey(), e.getValue());
        });

        return finalDst;
    }
}
