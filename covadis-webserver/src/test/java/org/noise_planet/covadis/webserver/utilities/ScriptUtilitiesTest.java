/*
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and
 * education, as well as by experts in a professional use.
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE
 * provided with this software.
 *
 * Official webpage : http://noise-planet.org/noisemodelling.html
 *  Contact: contact@noise-planet.org
 *
 */

package org.noise_planet.covadis.webserver.utilities;

import org.junit.jupiter.api.Test;
import org.noise_planet.covadis.scripts.CBS.ComputePerDepartment;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ScriptUtilitiesTest {
    @Test
    public void testFillDefaultValuesWithUnexpectedInputThrows() {
        Map inputs = new HashMap();
        inputs.put("projectionName", "hexa");
        inputs.put("unknownInput", "value");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ScriptUtilities.fillDefaultValues(ComputePerDepartment.class, inputs));
        assertTrue(ex.getMessage().contains("unknownInput"));
    }

    @Test
    public void testFillDefaultValuesWithDeclaredInputsSucceeds() {
        Map inputs = new HashMap();
        inputs.put("projectionName", "hexa");

        Map result = ScriptUtilities.fillDefaultValues(ComputePerDepartment.class, inputs);
        assertEquals("hexa", result.get("projectionName"));
        assertTrue(result.containsKey("projectionName"));
    }
}
