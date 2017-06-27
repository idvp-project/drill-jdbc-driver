package org.idvp.drill.jdbc.integration;

import javax.enterprise.inject.spi.CDI;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
@SuppressWarnings("unused")
public class CDIIntegration extends AbstractIntegration {

    @Override
    protected Object createInstance(Class<?> $class) {
        return CDI.current().select($class).get();
    }
}
