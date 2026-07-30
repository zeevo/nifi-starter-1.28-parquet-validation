package org.example.nifi.api;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/**
 * Contract between a processor and a controller service implementation. Bundled into
 * nifi-starter-nar alongside both. If the implementation and the processors are ever split
 * into separate NARs, this interface has to move into a NAR that is an ancestor of both.
 */
@Tags({"starter", "example"})
@CapabilityDescription("Transforms string values on behalf of a processor.")
public interface StarterService extends ControllerService {

    /**
     * @param value the value to transform
     * @return the transformed value
     */
    String transform(String value);

}
