<!-- En-tête -->
<header>
    <p>Auteur: Samuel Marsault</p>
</header>

# 🌍 NoiseModelling-OSM

<p align="center">
  <img src="https://media1.tenor.com/m/G38rWJ7c66MAAAAC/earth-world.gif" width="250" height="200" alt="earth">
</p>

<hr>

## 📖 Table of Contents

1. [Description](#-description)
2. [Project Usage](#-project-usage-create-noise-map)
   1. [Configuration and Requirements](#1--configuration-and-requirements)
      1. [NoiseModelling](#a--noisemodelling)
      2. [QGIS](#b--qgis)
      3. [GDAL](#c-gdal)
         1. [Windows](#i-windows)
         2. [Ubuntu](#ii-ubuntu)
         3. [Other Linux Versions](#iii-other-linux-versions)
   2. [Create Files](#2--create-files)
   3. [Use NoiseModelling](#3--use-noisemodelling)
      1. [Import the Folder](#1-import-the-folder)
      2. [Configure the Receivers](#2-configure-the-receivers)
      3. [Model the Noise](#3-model-the-noise)
      4. [Export the Results](#4-export-the-results)
   4. [View Results in QGIS](#4--view-results-in-qgis)
3. [Sources](#-sources)

## 📝 Description

The project aims to create a WPS block for NoiseModelling using geoClimate to create noise maps. Unfortunately, the project could not be finalized due to compatibility issues between GeoServer and geoClimate.

The current version used for geoClimate is `SNAPSHOT-1.0.1`.

## 🗺 Project Usage (Create Noise Map)

To create a noise map with this project, please follow the steps below.

### 1. ⚙️ Configuration and Requirements

To get started, you need to either fork the project and set it up locally or simply download the .jar file from the [Release](Release) section.

#### A. 🔊 NoiseModelling

You need to have NoiseModelling `4.x` installed on your computer, as well as Java JDK `11.x`. There is a tutorial for installing the Java JDK [here](https://noisemodelling.readthedocs.io/en/4.x/Requirements.html) and instructions for installing NoiseModelling [here](https://noisemodelling.readthedocs.io/en/4.x/Get_Started_GUI.html#step-1-download-noisemodelling). Only step 1 is necessary unless you want to learn how to use the software; in that case, I recommend following the subsequent steps.

#### B. 🌍 QGIS

You will also need the QGIS software. You can install it [here](https://www.qgis.org/en/site/forusers/download.html).

#### C. GDAL

##### I. Windows

Download [OSGeo4W here](http://download.osgeo.org/osgeo4w/osgeo4w-setup.exe).

1. Click on `Advanced Install` ⚙️ and then `Next` ➡️.
2. Click on `Install from Internet` 🌐 and then `Next` ➡️.
3. Continue with `Next` ➡️ unless you want to change the "Root Directory". Again, click `Next` ➡️ unless you want to change it, then select `Use System Proxy Settings` and click `Next` ➡️.
4. Select `https://download.osgeo.org` 🔗 and click `Next` ➡️.
5. In the search bar 🔍, look for `gdal` and under `Commandline_Utilities`, click on `Skip` ⏭️ for the package named `gdal: The GDAL/ORG library and commandline tool`. Ensure the version in the Current column is `3.9.0-1` or higher. Do the same for the package `gdal309-runtime: The GDAL/OGR 3.9 runtime library`.
   ‼️ **If you are using a different version like 3.0.8, take the runtime for that version, not 3.9 or others.**
6. Click `Next` ➡️ unless you want to install other packages.
7. After downloading, click `Finish` ✅. Then in the Windows search bar, type `Edit the system environment variables` 🔍 and click on it.
8. Click on `Environment Variables` 🛠️, then in `System Variables`, double-click on `Path` and click `New` ➕ to add the path where OSGeo4W is installed with the bin. If you didn't change anything, it should be `C:\OSGeo4W\bin`. ‼️ **Make sure to verify this.**
9. After that, click `OK` ✔️ three times to close the system properties.

<p align="center">
  <img src="https://media1.tenor.com/m/tt_QT0UP05MAAAAC/congrats-fireworks.gif" width="250" height="200" alt="congratulations">
</p>

The configuration is complete. If the following steps do not work, refer to troubleshooting steps.

##### II. Ubuntu

If you are on Ubuntu, follow the steps [described here](https://mothergeo-py.readthedocs.io/en/latest/development/how-to/gdal-ubuntu-pkg.html).

‼️ If after installing GDAL, you run

```shell
ogr2ogr --formats
```

and do not see `FlatGeobuf` or encounter an `error (core dump)`, run

```shell
sudo apt update
sudo apt upgrade
```

and try again. This should resolve the issue.

##### III. Other Linux Versions

Simply run:

```shell
sudo apt update
sudo apt upgrade
sudo apt install gdal-bin
```

### 2. 📁 Create Files

After completing the configuration and downloading the .jar file, place it in your desired directory. You can execute it with this command (while in the same directory):

```shell
java -jar GeoClimate_Tools-1.0.1-SNAPSHOT.jar -l "Urbach" -o "$(pwd)" -s 2154
```

‼️ **If it doesn't work locally, don't forget to replace the `-o` option with your path. For example, `-o "$(pwd)/output"` on Linux or `-o "$(pwd)\output"` on Windows.**

Here are the various options you can provide:

- `-l` or `--location` for the location. Example: "Paris". **(required)**
- `-o` or `--output` for the output folder. Example: "C:/temp/geoClimate". **(required)**
- `-s` or `--srid` for the target projection identifier. Example: 2154. **(optional)**
- `-d` or `--database` to save or not the temporary `.mv.db` file. Example: 1. **(optional)**

In the output folder you specified with the `-o` option, there should be a new folder named: `osm_(location name)`.

Inside, there should be 4 files:

- `building.geojson` 🏙
- `dem.geojson` 🔲
- `ground_acoustic.geojson` 🌱
- `road_traffic.geojson` 🛣️

### 3. 🛠️ Use NoiseModelling

Once the script is executed, you will have 4 files.

When you have finished, you can launch the NoiseModelling application.

‼️ **After launching the application, go to the `Database_Manageur` section, drag the `Clean_Database` block to the `Builder` tab, click on `Are you sure?`, check the box in the `Inputs` tab on the left side of the screen, and then click on `Run Process`.**

After doing this, verify by taking the `Display_Database` block, click on it, check the `showColumns` button, and then click on `Run Process`. If nothing appears, the configuration is ready!

<p align="center">
  <img src="https://media1.tenor.com/m/a1tcXEnPrBYAAAAC/gg-wp.gif" width="200" height="200" alt="GG">
</p>

Now we will create the files needed to view the noise map.

#### 1. Import the Folder

Go to the **`Import_and_Export`** section and select **`Import_Folder`**.
- In **`Path of the folder`**, specify the path to the previously generated folder (starting with `osm_`).
- In **`extension to import`**, enter **`geojson`**.
- Click **`Run Process`**.

If all goes

well, several tables will appear after re-running **`display_database`**.

⚠️ The table names below are valid only if you have not changed the names of the .geojson files.

#### 2. Configure the Receivers

Go to the **`Receivers`** section and select the desired grid (recommended: **`Delauney_Grid`**).
- In **`Sources table name`**, enter **`ROAD_TRAFFIC`**.
- In **`Buildings table name`**, enter **`BUILDING`**.
- Modify other fields if necessary, otherwise click **`Run Process`**.

You will get 2 new tables: **`RECEIVERS`** and **`TRIANGLES`**.

#### 3. Model the Noise

Go to the **`NoiseModelling`** section and select **`Noise_Level_from_Traffic`**.
- In **`Receivers table name`**, enter **`RECEIVERS`**.
- In **`Buildings table name`**, enter **`BUILDING`**.
- In **`Roads table name`**, enter **`ROAD_TRAFFIC`**.
- In **`Ground absorption table name`**, enter **`GROUND_ACOUSTIC`**.
- In **`DEM table name`**, enter **`DEM`**.
- Modify other fields if necessary, otherwise click **`Run Process`**.

You will get several new tables:
- **`LDAY_GEOM`**
- **`LDEN_GEOM`**
- **`LEVENING_GEOM`**
- **`NIGHT_GEOM`**

#### 4. Export the Results

Go to the **`Import_and_Export`** section and select **`Export_Table`**.
- In **`Name of the table`**, enter one of the 4 tables [above](#3-model-the-noise).
- In **`Path of the file want to export`**, specify the destination path (e.g., `C:Home\noiseModelling\output\lden.geojson`).

⚠️ Do not forget to include the file name and extension at the end of the export path.

### 4. 👀 View Results in QGIS

Now that everything is in place, launch the `QGIS` software.

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
- [GDAL](https://gdal.org/)
