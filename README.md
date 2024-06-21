# Projet Plamade

Plateforme plamade d'éxecution de traitements NoiseModelling sur serveur (ou cluster de calcul) avec accès restraint utilisant OpenID (google pour l'instant).

![Diagramme de flux](/documents/diag_flux.svg "Execution process")

## Prerequisites
This example requires that you configure a Google API resource before running.

1. Open the [Google API Credentials Console](https://console.developers.google.com/projectselector/apis/credentials?supportedpurview=project&angularJsUrl=%2Fprojectselector%2Fapis%2Fcredentials%3Fsupportedpurview%3Dproject&authuser=2)

2. Select `Create` in the dialog box to create a new API project.

3. Give the project a name.

4. In the `Create credentials` dropdown select `OAuth client ID`.

5. Follow the prompts for creating a client id.

6. When prompted for Application Type select `Web Application`

7. In the `Authorized redirect URIs` add: `http://localhost:9590/authenticator?client_name=GoogleOidcClient`

8. Click the `Create` button.

9. Copy the newly created client id and client secret in the configuration file located in *platform/build/resources/main/config.yaml*

## Running plamade server

Define memory to use with the command

export _JAVA_OPTIONS="-Xms4000m -Xmx28000m"

Go into computation_core folder `cd computation_core`

then run (in order to push dependencies files into `computation_core/build/install/computation_core/lib`)

`../gradlew clean build installDist --refresh-dependencies`

Go into project root folder `cd ..`

Run ./gradlew platform:run

Copy the configuration file located in `platform/src/ratpack/config.demo.yaml` to `platform/src/ratpack/config.yaml` and edit with your auth credentials.

Run ./gradlew platform:run

Once the application has started, point your web browser to [http://localhost:9590](http://localhost:9590) to access the platform

## Ngnix configuration and https

Example of nginx configuration for setting up a https connection in production

```nginx
server {

        server_name mydomainname.org;

        root /var/www/html;
        index index.html;

        location / {
          proxy_set_header Host $host;
          proxy_set_header X-Real-IP $remote_addr;
          proxy_pass http://localhost:9590;
          proxy_set_header X-Forwarded-Host $host:$server_port;
          proxy_set_header X-Forwarded-Server $host;
          proxy_set_header X-Forwarded-Proto https;
          proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
          proxy_read_timeout 3600;
          proxy_connect_timeout 3600;
          proxy_send_timeout 3600;
        }
}
```











