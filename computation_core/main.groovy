import jnius_config
import os
import time
jnius_config.add_options('-Xmx4096m')
jnius_config.set_classpath('dependencies/*')
from jnius import autoclass

# Imports
GroovyShell = autoclass('groovy.lang.GroovyShell')
File = autoclass('java.io.File')


shell = GroovyShell().parse(File( '../script_groovy/1_Extract_Department.groovy' ))


