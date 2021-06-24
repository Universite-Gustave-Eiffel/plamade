# Projet Plamade

Plateforme plamade d'éxecution de traitements NoiseModelling sur serveur (ou cluster de calcul) avec accès restraint utilisant OpenID (google pour l'instant).

## Prerequisites
This example requires that you configure a Google API resource before running.

1. Open the [Google API Credentials Console](https://console.developers.google.com/projectselector/apis/credentials?supportedpurview=project&angularJsUrl=%2Fprojectselector%2Fapis%2Fcredentials%3Fsupportedpurview%3Dproject&authuser=2)

2. Select `Create` in the dialog box to create a new API project.

3. Give the project a name.

4. In the `Create credentials` dropdown select `OAuth client ID`.

5. Follow the prompts for creating a client id.

6. When prompted for Application Type select `Web Application`

7. In the `Authorized redirect URIs` add: `http://localhost:5050/authenticator?client_name=GoogleOidcClient`

8. Click the `Create` button.

9. Copy the newly created client id and client secret for use in the example.

## Running the Example
The example can be run using the following Gradle command:

    $ RATPACK_AUTH__CLIENT_ID={client id} RATPACK_AUTH__CLIENT_SECRET={client secret} ./gradlew run

Once the application has started, point your web browser to [http://localhost:5050](http://localhost:5050) to access the test page.
