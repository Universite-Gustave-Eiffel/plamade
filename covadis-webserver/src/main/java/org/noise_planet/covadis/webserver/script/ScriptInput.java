package org.noise_planet.covadis.webserver.script;

/**
 * Represents an input configuration for a script.
 * This class is designed to encapsulate the metadata and properties that define
 * an input to a script, such as its identifier, title, description, type, and
 * whether it is optional.
 */
public class ScriptInput {
    public String id;
    public String title;
    public String description;
    public String type;
    public boolean optional = false;
}
