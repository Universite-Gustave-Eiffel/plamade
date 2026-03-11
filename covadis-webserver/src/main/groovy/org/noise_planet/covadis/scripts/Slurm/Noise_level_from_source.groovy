/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */
package org.noise_planet.covadis.scripts.Slurm

import groovy.sql.Sql
import groovy.transform.Field
import org.apache.sshd.scp.client.ScpClientCreator
import org.apache.sshd.scp.common.helpers.ScpTimestampCommandDetails
import org.h2gis.api.EmptyProgressVisitor
import org.h2gis.api.ProgressVisitor
import org.noise_planet.covadis.scripts.Import_and_Export.Import_File
import org.noise_planet.covadis.webserver.slurm.FileAttributes
import org.noise_planet.covadis.webserver.slurm.SlurmConfig
import org.noise_planet.covadis.webserver.slurm.SlurmSession
import org.noise_planet.covadis.webserver.slurm.SlurmUtilities
import org.slf4j.LoggerFactory

import javax.sql.DataSource
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileTime
import java.nio.file.attribute.PosixFilePermissions
import java.sql.Connection
import java.sql.Statement
import java.time.LocalDateTime
import java.util.concurrent.CancellationException
import java.util.concurrent.atomic.AtomicLong

title = 'Computes the propagation from the sounds sources to the receivers'
description = '&#10145;&#65039; Computes the propagation from the sounds sources to the receivers location using the noise emission table on a remote HPC cluster using Slurm for job management.' +
        '<hr>' +
        '&#127757; Tables must be projected in a metric coordinate system (SRID). Use "Change_SRID" WPS Block if needed. </br></br>' +
        '&#x2705; The output table are called: <b> RECEIVERS_LEVEL </b> </br></br>' +
        'The output table contain: </br> <ul>' +
        '<li><b> IDRECEIVER</b>: receiver an identifier (INTEGER) linked to RECEIVERS table primary key</li>' +
        '<li><b> IDSOURCE</b>: source identifier (INTEGER) linked to SOURCES_GEOM primary key. Only if Keep source id is checked.</li>' +
        '<li><b> PERIOD </b>: Time period (VARCHAR) ex. L D E and DEN. Only if you provide emission power to sources or the atmospheric settings table.</li>' +
        '<li><b> THE_GEOM </b>: the 3D geometry of the receivers with the Z as the altitude (POINTZ)</li>' +
        '<li><b> Hz63, Hz125, Hz250, Hz500, Hz1000,Hz2000, Hz4000, Hz8000 </b>: 8 columns giving the sound level for each octave band (FLOAT)</li></ul>'

inputs = [
        tableBuilding           : [
                name       : 'Buildings table name',
                title      : 'Buildings table name',
                description: '&#127968; Name of the Buildings table</br> </br>' +
                        'The table must contain: </br><ul>' +
                        '<li><b> THE_GEOM </b>: the 2D geometry of the building (POLYGON or MULTIPOLYGON)</li>' +
                        '<li><b> HEIGHT </b>: the height of the building (FLOAT)</li>' +
                        '<li><b> G </b>: Optional, Wall absorption value if g is [0, 1] or wall surface impedance' +
                        ' ([N.s.m-4] static air flow resistivity of material) if G is [20, 20000]' +
                        ' (default is 0.1 if the column G does not exists) (FLOAT)</li></ul>',
                type       : String.class
        ],
        tableSources            : [
                name       : 'Sources geometry table name',
                title      : 'Sources geometry table name',
                description: 'Name of the Sources table (if only geometry is specified) </br> </br>' +
                        'The table must contain (* mandatory): </br> <ul>' +
                        '<li> <b> PK *</b> : an identifier. It shall be a primary key (INTEGER, PRIMARY KEY) </li> ' +
                        '<li> <b> THE_GEOM *</b> : the 3D geometry of the sources (POINT, MULTIPOINT, LINESTRING, MULTILINESTRING). According to CNOSSOS-EU, you need to set a height of 0.05 m for a road traffic emission </li> ' +
                        '<li> <b> HZD63, HZD125, HZD250, HZD500, HZD1000, HZD2000, HZD4000, HZD8000 </b> : 8 columns giving the day emission sound level for each octave band (FLOAT) </li> ' +
                        '<li> <b> HZE </b> : 8 columns giving the evening emission sound level for each octave band (FLOAT) </li> ' +
                        '<li> <b> HZN </b> : 8 columns giving the night emission sound level for each octave band (FLOAT) </li> ' +
                        '<li> <b> YAW </b> : Source horizontal orientation in degrees. For points 0&#176; North, 90&#176; East. For lines 0&#176; line direction, 90&#176; right of the line direction.  (FLOAT) </li> ' +
                        '<li> <b> PITCH </b> : Source vertical orientation in degrees. 0&#176; front, 90&#176; top, -90&#176; bottom. (FLOAT) </li> ' +
                        '<li> <b> ROLL </b> : Source roll in degrees (FLOAT) </li> ' +
                        '<li> <b> DIR_ID </b> : identifier of the directivity sphere from tableSourceDirectivity parameter or train directivity if not provided -> OMNIDIRECTIONAL(0), ROLLING(1), TRACTIONA(2), TRACTIONB(3), AERODYNAMICA(4), AERODYNAMICB(5), BRIDGE(6) (INTEGER) </li> </ul> ' +
                        '&#128161; This table can be generated from the WPS Block "Road_Emission_from_Traffic"',
                type       : String.class
        ],
        tableSourcesEmission            : [
                name       : 'Sources emission table name',
                title      : 'Sources emission table name',
                description: 'Name of the Sources table (ex. SOURCES_EMISSION) </br> </br>' +
                        'The table must contain: </br> <ul>' +
                        '<li><b> IDSOURCE </b>* : an identifier. It shall be linked to the primary key of tableRoads (INTEGER)</li>' +
                        '<li><b> PERIOD </b>* : Time period, you will find this column on the output (VARCHAR)</li>' +
                        '<li> <b> HZ63, HZ125, HZ250, HZ500, HZ1000, HZ2000, HZ4000, HZ8000 </b> : Emission noise level in dB can be third-octave 50Hz to 10000Hz (FLOAT) </li> ',
                min        : 0, max: 1, type: String.class
        ],
        tableReceivers          : [
                name       : 'Receivers table name',
                title      : 'Receivers table name',
                description: 'Name of the Receivers table </br> </br>' +
                        'The table must contain: </br> <ul>' +
                        '<li> <b> PK </b> : an identifier. It shall be a primary key (INTEGER, PRIMARY KEY) </li> ' +
                        '<li> <b> THE_GEOM </b> : the 3D geometry of the sources (POINT, MULTIPOINT) </li> </ul>' +
                        '&#128161; This table can be generated from the WPS Blocks in the "Receivers" folder',
                type       : String.class
        ],
        tableDEM                : [
                name       : 'DEM table name',
                title      : 'DEM table name',
                description: 'Name of the Digital Elevation Model (DEM) table </br> </br>' +
                        'The table must contain: </br> <ul>' +
                        '<li> <b> THE_GEOM </b> : the 3D geometry of the sources (POINT, MULTIPOINT) </li> </ul>' +
                        '&#128161; This table can be generated from the WPS Block "Import_Asc_File"',
                min        : 0, max: 1, type: String.class
        ],
        tableGroundAbs          : [
                name       : 'Ground absorption table name',
                title      : 'Ground absorption table name',
                description: 'Name of the surface/ground acoustic absorption table </br> </br>' +
                        'The table must contain: </br> <ul>' +
                        '<li> <b> THE_GEOM </b>: the 2D geometry of the sources (POLYGON or MULTIPOLYGON) </li>' +
                        '<li> <b> G </b>: the acoustic absorption of a ground (FLOAT between 0 : very hard and 1 : very soft) </li> </ul> ',
                min        : 0, max: 1, type: String.class
        ],
        tableSourceDirectivity          : [
                name       : 'Source directivity table name',
                title      : 'Source directivity table name',
                description: 'Name of the emission directivity table </br> </br>' +
                        'If not specified the default is train directivity of CNOSSOS-EU</b> </br> </br>' +
                        'The table must contain the following columns: </br> <ul>' +
                        '<li> <b> DIR_ID </b>: identifier of the directivity sphere (INTEGER) </li> ' +
                        '<li> <b> THETA </b>: [-90;90] Vertical angle in degree. 0&#176; front 90&#176; top -90&#176; bottom (FLOAT) </li> ' +
                        '<li> <b> PHI </b>: [0;360] Horizontal angle in degree. 0&#176; front 90&#176; right (FLOAT) </li> ' +
                        '<li> <b> HZ63, HZ125, HZ250, HZ500, HZ1000, HZ2000, HZ4000, HZ8000 </b>: attenuation levels in dB for each octave or third octave (FLOAT) </li> </ul> ' ,
                min        : 0, max: 1, type: String.class
        ],
        tablePeriodAtmosphericSettings          : [
                name       : 'Atmospheric settings table name for each time period',
                title      : 'Atmospheric settings table name for each time period',
                description: 'Name of the Atmospheric settings table </br> </br>' +
                        'The table must contain the following columns: </br> <ul>' +
                        '<li> <b> PERIOD </b>: time period (VARCHAR PRIMARY KEY) </li> ' +
                        '<li> <b> WINDROSE </b>: probability of occurrences of favourable propagation conditions (ARRAY(16)) </li> ' +
                        '<li> <b> TEMPERATURE </b>: Temperature in celsius (FLOAT) </li> ' +
                        '<li> <b> PRESSURE </b>: air pressure in pascal (FLOAT) </li> ' +
                        '<li> <b> HUMIDITY </b>: air humidity in percentage (FLOAT) </li> ' +
                        '<li> <b> GDISC </b>: choose between accept G discontinuity or not (BOOLEAN) default true </li> ' +
                        '<li> <b> PRIME2520 </b>: choose to use prime values to compute eq. 2.5.20 (BOOLEAN) default false </li> ' +
                        '</ul>' ,
                min        : 0, max: 1, type: String.class
        ],
        paramWallAlpha          : [
                name       : 'wallAlpha',
                title      : 'Wall absorption coefficient',
                description: 'Wall absorption coefficient (FLOAT) </br> </br>' +
                        'This coefficient is going <br> <ul>' +
                        '<li> from 0 : fully absorbent </li>' +
                        '<li> to strictly less than 1 : fully reflective. </li> </ul>' +
                        '&#128736; Default value: <b>0.1 </b> ',
                min        : 0, max: 1, type: String.class
        ],
        confReflOrder           : [
                name       : 'Order of reflexion',
                title      : 'Order of reflexion',
                description: 'Maximum number of reflections to be taken into account (INTEGER). </br> </br>' +
                        '&#x1F6A8; Adding 1 order of reflexion can significantly increase the processing time. </br> </br>' +
                        '&#128736; Default value: <b>1 </b>',
                min        : 0, max: 1, type: String.class
        ],
        confMaxSrcDist          : [
                name       : 'Maximum source-receiver distance',
                title      : 'Maximum source-receiver distance',
                description: 'Maximum distance between source and receiver (FLOAT, in meters). </br> </br>' +
                        '&#128736; Default value: <b>150 </b>',
                min        : 0, max: 1, type: String.class
        ],
        confMaxReflDist         : [
                name       : 'Maximum source-reflexion distance',
                title      : 'Maximum source-reflexion distance',
                description: 'Maximum reflection distance from the source (FLOAT, in meters). </br> </br>' +
                        '&#128736; Default value: <b>50 </b>',
                min        : 0, max: 1, type: String.class
        ],
        confThreadNumber        : [
                name       : 'Thread number',
                title      : 'Thread number',
                description: 'Number of thread to use on the computer (INTEGER). </br> </br>' +
                        'To set this value, look at the number of cores you have. </br>' +
                        'If it is set to 0, use the maximum number of cores available.</br> </br>' +
                        '&#128736; Default value: <b>0 </b>',
                min        : 0, max: 1, type: String.class
        ],
        confDiffVertical        : [
                name       : 'Diffraction on vertical edges',
                title      : 'Diffraction on vertical edges',
                description: 'Compute or not the diffraction on vertical edges. Following Directive 2015/996, enable this option for rail and industrial sources only. </br> </br>' +
                        '&#128736; Default value: <b>false </b>',
                min        : 0, max: 1, type: Boolean.class
        ],
        confDiffHorizontal      : [
                name       : 'Diffraction on horizontal edges',
                title      : 'Diffraction on horizontal edges',
                description: 'Compute or not the diffraction on horizontal edges. </br> </br>' +
                        '&#128736; Default value: <b>false </b>',
                min        : 0, max: 1, type: Boolean.class
        ],
        confExportSourceId      : [
                name       : 'Keep source id',
                title      : 'Separate receiver level by source identifier',
                description: 'Keep source identifier in output in order to get noise contribution of each noise source. </br> </br>' +
                        '&#128736; Default value: <b>false </b>',
                min        : 0, max: 1,
                type       : Boolean.class
        ],
        confHumidity            : [
                name       : 'Relative humidity',
                title      : 'Relative humidity',
                description: '&#127783; Humidity for noise propagation. </br> </br>' +
                        '&#128736; Default value: <b>70</b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        confTemperature         : [
                name       : 'Temperature',
                title      : 'Air temperature',
                description: '&#127777; Air temperature in degree celsius </br> </br>' +
                        '&#128736; Default value: <b> 15</b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        confFavourableOccurrencesDefault: [
                name       : 'Probability of occurrences',
                title      : 'Probability of occurrences',
                description: 'Comma-delimited string containing the default probability of occurrences of favourable propagation conditions. </br> </br>' +
                        'The north slice is the last array index not the first one <br/>' +
                        'Slice width are 22.5&#176;: (16 slices)</br> <ul>' +
                        '<li>The first column 22.5&#176; contain occurrences between 11.25 to 33.75 &#176; </li>' +
                        '<li>The last column 360&#176; contains occurrences between 348.75&#176; to 360&#176; and 0 to 11.25&#176; </li> </ul>' +
                        '&#128736; Default value: <b>0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5</b>',
                min        : 0, max: 1,
                type       : String.class
        ],
        confRaysName            : [
                name       : '',
                title      : 'Export scene',
                description: 'Save each mnt, buildings and propagation rays into the specified table (ex:RAYS) or file URL (ex: file:///Z:/dir/map.kml) </br> </br>' +
                        'You can set a table name here in order to save all the rays computed by NoiseModelling. </br> </br>' +
                        'The number of rays has been limited in this script in order to avoid memory exception. </br> </br>' +
                        '&#128736; Default value: <b>empty (do not keep rays)</b>',
                min        : 0, max: 1, type: String.class
        ],
        confMaxError            : [
                name       : 'Max Error (dB)',
                title      : 'Max Error (dB)',
                description: 'Threshold for excluding negligible sound sources in calculations. Default value: <b>0.1</b>',
                min        : 0, max: 1,
                type       : Double.class
        ],
        frequencyFieldPrepend            : [
                name       : 'Frequency field name',
                title      : 'Frequency field name',
                description: 'Frequency field name prepend. Ex. for 1000 Hz frequency the default column name is HZ1000.' +
                        '&#128736; Default value: <b>HZ</b>',
                min        : 0, max: 1, type: String.class
        ],
        configuration_name      : [
                name       : 'SSH Configuration Identifier',
                title      : 'SSH Configuration Identifier',
                description: 'Slurm configuration written through Write_HPC_Settings WPS Script',
                type       : String.class
        ],
        key_password        : [
                name       : 'SSH Private Key password',
                title      : 'SSH Private Key password',
                description: 'Optional private key password',
                min        : 0, max: 1,
                type       : String.class
        ],
]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type       : String.class
        ]
]

@Field
int POLL_SLURM_STATUS_TIME = 5000;
/**
 * Main run function
 * @param dataSource
 * @param inputs
 * @return
 */
def exec(DataSource dataSource, Map inputs, ProgressVisitor progress) {
    def noiseModellingDownloadURL = "https://github.com/Universite-Gustave-Eiffel/plamade/releases/download/v2.0.0-SNAPSHOT_2026_03_06/NoiseModellingCovadis-2.0.0-SNAPSHOT.zip"
    def noiseModellingFolder = noiseModellingDownloadURL.substring(noiseModellingDownloadURL.lastIndexOf('/') + 1, noiseModellingDownloadURL.lastIndexOf('.zip'))
    String jobIdentifier = Thread.currentThread().name
    // Create a logger with the thread name as it contains the Job identifier
    def logger = LoggerFactory.getLogger(jobIdentifier)
    File exportDir
    SlurmConfig slurmConfig
    try (Connection connection = dataSource.connection) {
        Sql sql = Sql.newInstance(connection)
        def res = sql.firstRow("SELECT * FROM SLURM_CONFIGURATION WHERE configuration_name = ?", inputs.configuration_name)
        slurmConfig = new SlurmConfig(host: res.host,
                port: res.port as Integer,
                sshKeyArmoredString: res.private_key,
                sshKeyPassword: inputs.key_password,
                user: res.user_name,
                serverKey: res.ssl_key,
                serverKeyType: res.ssh_key_type,
                javaBinaryPath: res.java_binary_path)
        exportDir = exportDatabase(connection)
        exportDir.deleteOnExit()
    }

    try(SlurmSession slurmSession = new SlurmSession(slurmConfig, logger)) {
        slurmSession.connect()

        validateJavaVersion(slurmSession, slurmConfig.javaBinaryPath)
        setupNoiseModelling(slurmSession, noiseModellingDownloadURL, noiseModellingFolder)

        slurmConfig.serverWorkspaceFolder = createRemoteWorkspace(slurmSession, jobIdentifier)

        uploadFile(slurmSession, new File(exportDir, "h2database.zip"), slurmConfig.serverWorkspaceFolder+ "/h2database.zip")

        def bashCode = generateSlurmBashScript(new File(slurmConfig.javaBinaryPath).parentFile.parent, "~/"+noiseModellingFolder, inputs, slurmConfig.serverWorkspaceFolder)

        // upload bash script
        createRemoteFile(bashCode, slurmSession, "${slurmConfig.serverWorkspaceFolder}/noisemodelling_batch.sh")

        // Fetch the job id from the output of the sbatch command
        def response = slurmSession.runCommand("cd ${slurmConfig.serverWorkspaceFolder} && sbatch --array=1-${slurmConfig.maxTasksPerJobs} noisemodelling_batch.sh", true).first()
        def matcher = (response =~ /Submitted batch job (\d+)/)
        if (matcher) {
            slurmConfig.jobId = Integer.parseInt(matcher[0][1] as String) // [index of match][index of capture group]
        } else {
            return ["result": "No job ID found"]
        }
        logger.info("Slurm job submitted with id {}", slurmConfig.jobId)
        // Wait some time before fetching logs
        Thread.sleep(1000)
        // Capture logs of all the tasks
        Map<String, Long> bytesReadInFiles = new HashMap<>();

        ProgressVisitor tasksProgress = progress.subProcess(slurmConfig.maxTasksPerJobs)
        while(true) {
            if (progress.isCanceled()) {
                // User cancel the computation, cancel the job on the remote slurm cluster
                slurmSession.runCommand(String.format("scancel %d", slurmConfig.jobId), true)
                throw new CancellationException("The job has been canceled as requested by the user")
            }
            long lastPullTime = System.currentTimeMillis();
            // Log task outputs
            SlurmUtilities.logSlurmJobs(slurmSession, slurmConfig.serverWorkspaceFolder, bytesReadInFiles);

            // Check status of jobs on cluster side
            if (slurmSession.updateSlurmJobProgression(tasksProgress)) {
                // All tasks done or the main task failed
                break;
            }

            Thread.sleep(Math.max(1000, POLL_SLURM_STATUS_TIME - (System.currentTimeMillis() - lastPullTime)))
        }
        // All the tasks are completed download results in a temporary directory
        List<String> output = slurmSession.runCommand(String.format("find %s/*.geojson -type f -printf \"%%s,%%f\\n\"",
                slurmConfig.serverWorkspaceFolder), false);
        List<FileAttributes> files = SlurmUtilities.parseLSCommand(output)
        List<String> remoteFiles = new ArrayList<>()
        for (FileAttributes file : files) {
            remoteFiles.add(new File(slurmConfig.serverWorkspaceFolder ,file.fileName).getPath())
        }
        def temporaryDataDir = Files.createTempDirectory(getDateString())
        downloadFiles(slurmSession, remoteFiles.toArray(new String[0]), temporaryDataDir)
        // Transfer results into a single table
        try (Connection connection = dataSource.connection) {
            Queue<File> filesToImport = new ArrayDeque<>()
            for (FileAttributes file : files) {
                filesToImport.add(new File(temporaryDataDir.toFile() ,file.fileName))
            }
            // Import the first file as usual
            new Import_File().exec(connection, [pathFile : filesToImport.pop().getPath(), tableName : "RECEIVERS_LEVEL"], new EmptyProgressVisitor())
            // Link with external file for the others then copy the rows into the merged table
//            Sql sql = Sql.newInstance(connection)
//            int uniqueIndex = 1
//            while (!filesToImport.isEmpty()) {
//                File localFile = filesToImport.pop()
//                def tableName = "RECEIVERS_LEVEL_$uniqueIndex"
//                sql.execute("CALL FILE_TABLE('${localFile.getPath()}', '$tableName')".toString())
//                sql.execute("INSERT INTO RECEIVERS_LEVEL VALUES SELECT * FROM $tableName".toString())
//                sql.execute("DROP TABLE $tableName".toString())
//                uniqueIndex += 1
//            }
        }
        return ["result": "Setup completed"]
    }
}

/**
 * This batch script is executed for each Task, so database is copied and output data renamed using the task identifier
 * @param javaHome
 * @param noisemodellingPath
 * @param inputs
 * @param workspacePath
 * @return
 */
public static String generateSlurmBashScript(String javaHome, String noisemodellingPath, Map inputs, String workspacePath) {

    String tableReceivers = inputs['tableReceivers']

    // remove specific keys of this WPS process
    Map<String, Object> inputsCopy = new HashMap<>(inputs)
    inputsCopy.remove("key_password")
    inputsCopy.remove("configuration_name")

    // build arguments string for the local Noise_level_from_source.groovy script
    StringBuilder arguments = new StringBuilder()
    for(Map.Entry<String, Object> entry : inputsCopy.entrySet()) {
        arguments.append("-").append(entry.key)
        if(entry.value instanceof String) {
            arguments.append(" \"").append(entry.value).append("\" ")
        } else {
            arguments.append(" ").append(entry.value).append(" ")
        }
    }


    def script = $/#!/bin/sh
# run with this command
# must be run in the same folder than the database
# sbatch --array=0-11 noisemodelling_batch.sh

echo "copy data to local node storage"
unzip h2database.zip -d /scratch/job."$$SLURM_JOB_ID"/  || exit 1

echo "Prepare NoiseModelling run"

cd /scratch/job."$$SLURM_JOB_ID"/ || exit 1

#rename the h2 database
mv *.mv.db h2gisdb.mv.db || exit 1

export JAVA_HOME=$javaHome

echo "Filter receivers"
bash $noisemodellingPath/bin/ScriptRunner -w /scratch/job."$$SLURM_JOB_ID"/ -s $noisemodellingPath/scripts/Slurm/FilterTaskReceivers.groovy -taskId "$$SLURM_ARRAY_TASK_ID" -minTaskId "$$SLURM_ARRAY_TASK_MIN" -maxTaskId "$$SLURM_ARRAY_TASK_MAX" -tableReceivers $tableReceivers || exit 1

echo "Run propagation"
bash $noisemodellingPath/bin/ScriptRunner -w /scratch/job."$$SLURM_JOB_ID"/ -s $noisemodellingPath/scripts/NoiseModelling/Noise_level_from_source.groovy $arguments || exit 1

echo "Export results"
bash $noisemodellingPath/bin/ScriptRunner -w /scratch/job."$$SLURM_JOB_ID"/ -s $noisemodellingPath/scripts/Import_and_Export/Export_Table.groovy -exportPath $workspacePath/RECEIVERS_LEVEL_"$$SLURM_ARRAY_TASK_ID".geojson -tableToExport RECEIVERS_LEVEL || exit 1
/$
    return script
}

private static void validateJavaVersion(SlurmSession slurmSession, String javaBinaryPath) {
    def outputs = slurmSession.runCommand("${javaBinaryPath} --version", true)
    def mainVersion = (outputs.first() =~ /\d+/)[0] as Integer
    if (mainVersion < 11) throw new IllegalStateException("Java version $mainVersion < 11")
    slurmSession.runCommand("export JAVA_HOME=${new File(javaBinaryPath).parentFile.parent}", true)
}

private static void setupNoiseModelling(SlurmSession slurmSession, String nmUrl, String nmFolder) {
    if (!isRemoteFolderExists(slurmSession, "~/${nmFolder}")) {
        slurmSession.runCommand("wget ${nmUrl}", true)
        slurmSession.runCommand("unzip -o ${nmFolder}.zip -d ${nmFolder} && rm ${nmFolder}.zip", true)
        if (!isRemoteFolderExists(slurmSession, "~/${nmFolder}")) {
            throw new IllegalStateException("NoiseModelling folder not found after unzipping")
        }
    }
}

private static File exportDatabase(Connection connection) {
    def exportDir = Files.createTempDirectory(getDateString()).toFile()
    connection.createStatement().execute("BACKUP TO '${exportDir.absolutePath}/h2database.zip'")
    return exportDir
}

private static String createRemoteWorkspace(SlurmSession slurmSession, String jobIdentifier) {
    def remoteWorkspace = "~/workspace/${getDateString()}_${jobIdentifier}"
    String command = "mkdir -p -v ${remoteWorkspace}"
    slurmSession.getLogger().info("Run command {}", command)
    slurmSession.runCommand(command, true)
    return remoteWorkspace
}

private static void uploadFile(SlurmSession slurmSession, File localFile, String remotePath) {
    def scpClient = ScpClientCreator.instance().createScpClient(slurmSession.getSession())
    scpClient.upload(localFile.absolutePath, remotePath)
}

static void downloadFiles(SlurmSession slurmSession, String[] remote, Path local) {
    def scpClient = ScpClientCreator.instance().createScpClient(slurmSession.getSession())
    scpClient.download(remote, local)
}

private static void createRemoteFile(String fileContent, SlurmSession slurmSession, String remoteFileName) {
    def scpClient = ScpClientCreator.instance().createScpClient(slurmSession.getSession())
    scpClient.upload(new ByteArrayInputStream(fileContent.getBytes(Charset.defaultCharset())),
            remoteFileName, fileContent.length(), PosixFilePermissions.fromString("rwxr-x---"),
            new ScpTimestampCommandDetails(FileTime.fromMillis(System.currentTimeMillis()),
                    FileTime.fromMillis(System.currentTimeMillis())))
}

private static boolean isRemoteFolderExists(SlurmSession slurmSession, String folder) {
    String command = "[ -d $folder ] && echo \"Exists\" || echo \"Not found\""
    slurmSession.getLogger().info("Run command {}", command)
    slurmSession.runCommand(command, true, new AtomicLong()).first() == "Exists"
}

private static String getDateString() {
    def now = LocalDateTime.now()
    "${now.year}_${now.monthValue}_${now.dayOfMonth}.${now.hour}.${now.minute}"
}
