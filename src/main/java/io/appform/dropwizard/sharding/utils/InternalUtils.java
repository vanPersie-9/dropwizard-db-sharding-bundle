package io.appform.dropwizard.sharding.utils;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.val;

import java.io.*;

/**
 * Utilities for internal use
 */
@UtilityClass
public class InternalUtils {
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T cloneObject(final T input) {
        try (val bos = new ByteArrayOutputStream(); val os = new ObjectOutputStream(bos)) {
            os.writeObject(input);
            os.flush();

            try(val ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
                return (T) ois.readObject();
            }
        }
    }
}
