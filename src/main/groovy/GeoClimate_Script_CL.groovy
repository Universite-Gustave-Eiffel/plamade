import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovyjarjarpicocli.CommandLine
import java.util.concurrent.Callable

//Essayer de mettre le .jar de géoclimate et le faire en dépendance pour pas utiliser celle de l'internet.

@CommandLine.Command(name = 'GeoClimate_Script_CL', mixinStandardHelpOptions = true, version = '1.0',
        description = 'Script for run Import_GeoClimate_Data')

class GeoClimate_Script_CL implements Callable<Integer> {

    private static final Integer SUCCESS = 0

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
    Integer call() throws Exception {
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
            //logger.info('ERROR : '+ e.toString())
            return ERROR
        }
        return SUCCESS
    }

    static void main(String[] args) {
        int exitCode = new CommandLine(new GeoClimate_Script_CL()).execute(args)
        System.exit(exitCode)
    }

}
