package org.noise_planet.plamade

import groovy.sql.Sql
import org.junit.Test
import org.noise_planet.plamade.process.NoiseModellingInstance

import javax.sql.DataSource
import java.sql.Connection
import java.text.CharacterIterator
import java.text.StringCharacterIterator

class testPerf {
    public static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format(Locale.ROOT, "%.1f %cB", bytes / 1000.0, ci.current());
    }

    @Test
    public void testCell() {
        long heapMaxSize = Runtime.getRuntime().maxMemory();
        System.out.println("Max memory: " + humanReadableByteCountSI(heapMaxSize))
        // Begin processing of cell 483 / 625
        // This computation area contains 93116 receivers 192 sound sources and 6453 buildings
        DataSource ds = NoiseModellingInstance.createDataSource("", "",
                "C:\\Users\\kento\\softs\\noisemodelling_plamade_3.4.1\\data_dir", "h2gisdb")
        ds.getConnection().withCloseable {
            Connection connection ->
                Sql sql = new Sql(connection)
                def conf = sql.firstRow("SELECT CONFID, CONFREFLORDER, CONFMAXSRCDIST, CONFMAXREFLDIST, CONFDISTBUILDINGSRECEIVERS, CONFTHREADNUMBER, CONFDIFFVERTICAL, CONFDIFFHORIZONTAL, CONFSKIPLDAY, CONFSKIPLEVENING, CONFSKIPLNIGHT, CONFSKIPLDEN, CONFEXPORTSOURCEID, WALL_ALPHA\n" +
                        "FROM PUBLIC.CONF WHERE CONFID = 3")

                GroovyShell shell = new GroovyShell()
                Script receiversGrid= shell.parse(new File("script_groovy", "s4_Rail_Noise_level.groovy"))
                Map<String, Object> inputs = new HashMap<>()
                inputs.put("confId", conf.CONFID)
                def result = receiversGrid.invokeMethod("exec", [connection, inputs])
                System.out.println(result)
        }
    }
}
