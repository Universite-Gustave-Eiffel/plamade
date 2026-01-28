# NoiseModelling Covadis

This repository host the NoiseModelling-Covadis Platform, this is a strategic noise maps generator using NoiseModelling computation
core and Covadis environemental database specification French standard.

COVADIS, the Commission for the Validation of Spatial Data, is a French interministerial commission set up by the
ministries responsible for ecology, housing and agriculture to standardise the geographical data most frequently used in
their fields.

The Noise Directive 2002/49/EC aims to establish a common approach to avoiding, preventing or reducing
the harmful effects of environmental noise. Member States are therefore required to produce noise maps
every five years, at fixed intervals, using common assessment methods. This requires
characterising noise pollution emitted by roads, railways, airports and urban areas
that meet certain threshold criteria. To this end, in France, all the data needed to produce
strategic noise maps for non-concessionary roads and railways (traffic, buildings, population,
relief, etc.) has been centralised in a national database called PlaMADE (Plateforme mutualisée d'aide
au diagnostic environnemental), in a single format that complies with the COVADIS GeoStandard "Noise in the Environment
". Based on this data, and using the open-source noise propagation calculation tool
NoiseModelling, all noise indicators and maps are produced.

The goal is to make a web platform able to quickly generate strategic noise maps on the country level scale using
input/output data following accurate specification.

This platform may use High Power Computing platform (Slurm) in order to run the most computing intensive tasks.

![Diagramme de flux](/documents/diag_flux.svg "Execution process")

# Deployment

This platform is deployed using Docker and hosted on the Github Packages.

On the root of this repository you can find an example docker compose.

If you have a domain name you can use the environment variable PROXY_BASE_URL with your domain name.

By default the service is accessible from the path /nmcovadis but you can change it by using the environment variable ROOT_URL (empty to use the base url)

## Dependencies

Install Docker or Podman on your system

## Running

Download the file [docker-compose.yml](docker-compose.yml) and run this command in the same folder:

```bash
docker compose up -d
```

or

```bash
podman compose up -d
```

Follow the instructions of the logs in order to register the administrator account.

```bash
docker compose logs noisemodelling
```

or

```bash
podman compose logs noisemodelling
```




