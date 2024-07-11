# Initialize Plamade

1. Fork the Plamade repository, then clone it or download it as a .zip file (not recommended).

2. Follow the steps in the [README.md](README.md) up to the end of point 4 in the `Prerequisites` section.

3. Create and configure the OAuth consent screen (fill in the fields marked with an "*", then skip steps 2, 3, and 4).

4. Once that's done, complete steps 4 to 8 in the [README.md](README.md).

5. After finishing, download the JSON file.

6. In the Plamade project, go to `platform\src\ratpack`, copy `config.demo.yaml`, and paste it in the same folder, renaming it to `config.yaml`.

7. In the `clientId` field, enter the client ID obtained from the JSON file, and do the same for `clientSecret`.

8. If you don't know your admin password, follow step 9; otherwise, proceed to step 10.

9. Go to `platform\src\main\java\org.noise_planet.plamade\api\secure\GetJobList`, and after line 71, add: `LOG.info(String.format(Locale.ROOT,"User %s try to connect",profile.getId()));`

10. If you are using IntelliJ IDEA (recommended), click on the Gradle icon on the right side of the screen, then go to `computation_core\Tasks\distribution` and click `installDist`.

11. After that, still in Gradle, go to `platform\Tasks\application` and click `run`.

12. When the link appears, click on it (it should look like: http://localhost:9590).

13. Click on `Get Started`, choose your Google account, and accept the terms.

14. If you already have the admin key from earlier, go to step 16. Otherwise, for others, look in the IntelliJ IDEA console and scroll up until you see something like: `[ratpack-blocking-3-2] INFO  - User xxx try to connect`. Where `xxx` is, you should see a large number. Copy it and paste it into line 10 of the file you edited earlier where you put `clientID` and `clientSecret`.

15. Then, log out of Plamade, stop the program, and repeat steps 11, 12, and 13.

16. Go to `Admin` and click on the tab above `Logout`, then accept your account.

Well done, you have finished. (You can remove the line `LOG.info(String.format(Locale.ROOT,"User %s try to connect",profile.getId()));` that you added to the code earlier.)