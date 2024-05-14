import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import groovy.lang.GroovyShell
import groovy.lang.GroovyCodeSource
import Import_GeoClimate_Data

@Command(name = 'GeoClimate_Script_CL', mixinStandardHelpOptions = true, version = '1.0',
        description = 'Script for run Import_GeoClimate_Data')

class GeoClimate_Script_CL implements Runnable {

    public static final def ERROR = 0

    @Option(names = ['-l', '--location'], description = 'Location of City or Street', required = true)
    String location

    @Option(names = ['-o', '--output'], description = 'Path of folder you want output result', required = true)
    String path

    @Option(names = ['-s', '--srid'], description = 'Target projection identifier (also called SRID) of your table', defaultValue = '2154')
    Integer srid

    @Option(names = ['-d', '--database'], description = 'Database use by géoClimate for create files (is a .mv.db)', defaultValue = 'true')
    Boolean dataBase

    static class Input {
        String locations
        String filesExportPath
        Integer targetSRID
        Boolean geoclimatedb
    }

    @Override
    void run() {
        Input input = new Input(
                locations: location,
                filesExportPath: path,
                targetSRID: srid,
                geoclimatedb: dataBase
        )

        try {
            Import_GeoClimate_Data.execWithCommandLine(input)
        } catch (e) {
            println('ERROR : '+ e.toString())
        }

    }

    static void main(String[] args) {
        int exitCode = new CommandLine(new GeoClimate_Script_CL()).execute(args)
        System.exit(exitCode)
    }
}
