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
package org.noise_planet.covadis.scripts.hpc

import org.h2gis.api.ProgressVisitor
import org.noise_planet.noisemodelling.pathfinder.utils.profiler.RootProgressVisitor

import javax.sql.DataSource

title = 'Write HPC settings'
description = 'Create a table that will contain all settings to connect to a Slurm service through SSH'

title = 'Computes the propagation from the sounds sources to the receivers'
description = '&#10145;&#65039; Computes the propagation from the sounds sources to the receivers location using the noise emission table.' +
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
        ]
]

outputs = [
        result: [
                name       : 'Result output string',
                title      : 'Result output string',
                description: 'This type of result does not allow the blocks to be linked together.',
                type       : String.class
        ]
]

/**
 * Main run function
 * @param dataSource
 * @param input
 * @return
 */
def exec(DataSource dataSource, Map input) {

    ProgressVisitor progressLogger

    if("_progression" in input) {
        progressLogger = input["_progression"] as ProgressVisitor
    } else {
        progressLogger = new RootProgressVisitor(1, true, 1)
    }



    // print to WPS Builder
    return ["result" : ""]

}

