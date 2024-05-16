# NoiseModelling-OSM
## Description

The project is to create WPS block for noiseModelling using géoClimate for create noise map

The current version use for géoClimate is the SNAPSHOT-1.0.2 because the 1.0.1 not work.

There's also a scrpit to run the main script with command lines. 
However, due to a dependency problem, it still doesn't work when run in a terminal (it works fine in an IDE with parameter configuration).

There are 4 options. Options are :
- "-l" or "--location" for the location. Example : "Paris". **(required)**
- "-o" or "--output" for the output folder. Examble : "C:/temp/geoClimate". **(required)**
- "-s" or "--srid" for the Target projection identifier. Exemple : 2154. **(optional)**
- "-d" or "--database" for save or not the temp file .mv.db. Example : 1. **(optional)**

### Sources
- [noiseModelling](https://github.com/Universite-Gustave-Eiffel/NoiseModelling)
- [geoClimate](https://github.com/orbisgis/geoclimate)

