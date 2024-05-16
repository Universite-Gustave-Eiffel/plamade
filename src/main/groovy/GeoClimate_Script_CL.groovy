import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovyjarjarpicocli.CommandLine

@CommandLine.Command(name = 'GeoClimate_Script_CL', mixinStandardHelpOptions = true, version = '1.0',
        description = 'Script for run Import_GeoClimate_Data')

class GeoClimate_Script_CL implements Runnable {

    private static final Integer ERROR = 1

    @CommandLine.Option(names = ['-l', '--location'], description = 'Location of City or Street', required = true)
    String location

    @CommandLine.Option(names = ['-o', '--output'], description = 'Path of folder you want output result', required = true)
    String path

    @CommandLine.Option(names = ['-s', '--srid'], description = 'Target projection identifier (also called SRID) of your table', defaultValue = '2154')
    Integer srid

    @CommandLine.Option(names = ['-d', '--database'], description = 'Database use by géoClimate for create files (is a .mv.db)', defaultValue = 'true')
    Boolean dataBase

    @Override
    void run() {

        Logger logger = LoggerFactory.getLogger("org.noise_planet.noisemodelling")

        def input = [
                "locations": location,
                "filesExportPath": path,
                "targetSRID": srid,
                "geoclimatedb": dataBase
        ]

        try {
            Import_GeoClimate_Data.execWithCommandLine(input)
        } catch (e) {
            logger.info('ERROR : '+ e.toString())
            System.exit(ERROR)
        }

    }

    static void main(String[] args) {
        int exitCode = new CommandLine(new GeoClimate_Script_CL()).execute(args)
        System.exit(exitCode)
    }
}
