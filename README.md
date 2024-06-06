# 🌍 NoiseModelling-OSM

<p align="center">
  <img src="https://media1.tenor.com/m/G38rWJ7c66MAAAAC/earth-world.gif" width="250" height="200" alt="earth">
</p>

## 📖 Table of Contents

1. [Description](#-description)
2. [Configuration and Use in the IDE](#-configuration-and-use-in-the-ide)
   1. [Available Options](#available-options)
3. [Project Usage](#-project-usage-create-noise-map)
   1. [Configuration and Requirements](#1--configuration-and-requirements)
   2. [Create Files](#2--create-files)
   3. [Use NoiseModelling](#3--use-noisemodelling)
      1. [Import the Folder](#1-import-the-folder)
      2. [Configure the Receivers](#2-configure-the-receivers)
      3. [Model the Noise](#3-model-the-noise)
      4. [Export the Results](#4-export-the-results)
   4. [View Results in QGIS](#4--view-results-in-qgis)
4. [Sources](#-sources)

## 📝 Description

The project aims to create a WPS block for NoiseModelling using geoClimate to create noise maps. Unfortunately, the project could not be finalized due to compatibility issues between GeoServer and geoClimate.

The current version used for geoClimate is `SNAPSHOT-1.0.1`.

## 🛠️ Configuration and Use in the IDE

Fork the project and then download it locally using Git.

If you do not have IntelliJ IDEA, [install it](https://www.jetbrains.com/idea/download/?section=windows).

Then open the project folder. Go to `File`, then `Open`, find and select the `GeoClimate_Tools` folder, and click `OK`.

‼️ There is a script [GeoClimate_Script_CL](src/main/groovy/GeoClimate_Script_CL.groovy) to run the main script [Import_GeoClimate_Data](src/main/groovy/Import_GeoClimate_Data.groovy) from the command line. However, due to a dependency issue, it still does not work when run in a terminal (it works fine in an IDE with parameter configuration).

To configure the command line run in IntelliJ:

1. To the left of the executable arrow, click on "Current File" and choose "Edit Configuration".
2. ➕ Add a new configuration and click on Groovy.
3. ✏️ Name it.
4. In "Script Path", enter the absolute path of the `GeoClimate_Script_CL` file.
5. In "Working directory", enter the absolute path of the `GeoClimate_Tools` directory.
6. In "Program arguments", enter the options described [below](#available-options). For example: `-l Urbach -o C:\noiseModelling\Block_WPS\GeoClimate_Tools\outPut\geoClimate -s 2154`.
7. 💾 Save and run.

<p align="center">
  <img src="https://media1.tenor.com/m/tt_QT0UP05MAAAAC/congrats-fireworks.gif" width="250" height="200" alt="congratulation">
</p>

In the output folder you specified, there should be a new folder named: `osm_(location name)`.

Inside, there should be 5 files:

- `building.geojson` 🏙
- `dem.geojson` 🔲
- `ground_acoustic.geojson` 🌱
- `road_traffic.geojson` 🛣️

If you want to test in the terminal to see the error, in the `GeoClimate_Tools` folder, run:

```shell
java -jar target/GeoClimate_Tools-1.0.1-SNAPSHOT.jar -l "Urbach" -o "$(pwd)\outPut\geoClimate" -s 2154
```

If it doesn't work locally, don't forget to replace the `-o` with your path.

🔧 If it still doesn't work, check that you have JDK 11.0.2 in your computer's environment variables.
Tutorial for Windows [here](https://confluence.atlassian.com/doc/setting-the-java_home-variable-in-windows-8895.html) or for all users [here](https://stackoverflow.com/questions/24641536/how-to-set-java-home-in-linux-for-all-users).

### Available Options

- `-l` or `--location` for the location. Example: "Paris". **(required)**
- `-o` or `--output` for the output folder. Example: "C:/temp/geoClimate". **(required)**
- `-s` or `--srid` for the target projection identifier. Example: 2154. **(optional)**
- `-d` or `--database` to save or not the temporary `.mv.db` file. Example: 1. **(optional)**

## 🗺 Project Usage (Create Noise Map)

To create a noise map with this project, please follow the steps below.

### 1. ⚙️ Configuration and Requirements

You need to have NoiseModelling `4.x` installed on your computer as well as Java JDK `11.x`.
There is a tutorial for installing the Java JDK [here](https://noisemodelling.readthedocs.io/en/4.x/Requirements.html)
and how to install NoiseModelling [here](https://noisemodelling.readthedocs.io/en/4.x/Get_Started_GUI.html#step-1-download-noisemodelling). Only step 1 is necessary unless you want to learn how to use the software; in that case, I recommend following the subsequent steps.

You will also need the QGIS software. You can install it [here](https://www.qgis.org/en/site/forusers/download.html).

If you are in windows, Vous devrez également télécharger [OSGeo4W here](http://download.osgeo.org/osgeo4w/osgeo4w-setup.exe)

(etape de configuration)

Of course, you will need this project on your computer and follow the configuration steps to launch the script [from this section](#-configuration-and-use-in-the-ide).

### 2. 📁 Create Files

Configure the run parameters in IntelliJ IDEA as described in [this section](#-configuration-and-use-in-the-ide) and make sure to set the options you want in the "Program arguments" section.

### 3. 🛠️ Use NoiseModelling

Once the script is executed, you will have 5 files.

When you have finished, you can launch the NoiseModelling application.

‼️ **After launching the application, go to the "Database_Manageur" section, drag the "Clean_Database" block to the "Builder" tab, click on "Are you sure?", check the box in the "Inputs" tab on the left side of the screen, and then click on "Run Process".**

After doing this, verify by taking the "Display_Database" block, click on it, check the "showColumns" button, and then click on "Run Process". If nothing appears, the configuration is ready!

<p align="center">
  <img src="https://media1.tenor.com/m/a1tcXEnPrBYAAAAC/gg-wp.gif" width="200" height="200" alt="GG">
</p>

Now we will create the files needed to view the noise map.

#### 1. Import the Folder

Go to the **"Import_and_Export"** section and select **"Import_Folder"**.
- In **"Path of the folder"**, specify the path to the previously generated folder (starting with "osm_").
- In **"extension to import"**, enter **"geojson"**.
- Click **"Run Process"**.

If all goes well, several tables will appear after re-running **"display_database"**.

⚠️ The table names below are valid only if you have not changed the names of the .geojson files.

#### 2. Configure the Receivers

Go to the **"Receivers"** section and select the desired grid (recommended: **"Delauney_Grid"**).
- In **"Sources table name"**, enter **"ROAD_TRAFFIC"**.
- In **"Buildings table name"**, enter **"BUILDING"**.
- Modify other fields if necessary, otherwise click **"Run Process"**.

You will get 2 new tables: **RECEIVERS** and **TRIANGLES**.

#### 3. Model the Noise

Go to the **"NoiseModelling"** section and select **"Noise_Level_from_Traffic"**.
- In **"Receivers table name"**, enter **"RECEIVERS"**.
- In **"Buildings table name"**, enter **"BUILDING"**.
- In **"Roads table name"**, enter **"ROAD_TRAFFIC"**.
- In **"Ground absorption table name"**, enter **"GROUND_ACOUSTIC"**.
- In **"DEM table name"**, enter **"DEM"**.
- Modify other fields if necessary, otherwise click **"Run Process"**.

You will get several new tables:
- **LDAY_GEOM**
- **LDEN_GEOM**
- **LEVENING_GEOM**
- **NIGHT_GEOM**

#### 4. Export the Results

Go to the **"Import_and

_Export"** section and select **"Export_Table"**.
- In **"Name of the table"**, enter one of the 4 tables [above](#3-model-the-noise).
- In **"Path of the file want to export"**, specify the destination path (e.g., `C:Home\noiseModelling\output\lden.geojson`).

⚠️ Do not forget to include the file name and extension at the end of the export path.

### 4. 👀 View Results in QGIS

Now that everything is in place, launch the "QGIS" software.

In the `Layer` section, drag (if you wish) the output files to the end of [this step](#2--create-files). Next, drag one of the exported files from the [previous section](#4-export-the-results).

Again in the `Layer` section, right-click on the file you added (which has appeared) and click on `Properties`.

Go to `Symbology` and at the top, under `Single Symbol`, choose `Graduated`. In the `Value` dropdown list, select `LEAQ`.

In `Color ramp`, choose the one you want (though I recommend `Turbo`). You can also change some options like `Mode`, `Layer rendering`, `Method`, etc.
After that, click on the `Classify` button, then `Apply`, and finally `OK`.

Well done! You have finished the tutorial and have a wonderful noise map.

<p align="center">
  <img src="https://media1.tenor.com/m/mjUsjPWPcYYAAAAd/appreciate-well-done.gif" width="250" height="200" alt="nice job">
</p>

## 📚 Sources

- [NoiseModelling](https://github.com/Universite-Gustave-Eiffel/NoiseModelling)
- [geoClimate](https://github.com/orbisgis/geoclimate)
