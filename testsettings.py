import configparser

config = configparser.ConfigParser()
config.read("settings.config")

DEBUG = config["Debug"].getboolean("debug")

print config.sections()
for section in config.sections():
	print config[section]
	print config[section].keys()
	print config[section].values()
	print "End Section..."

if config["Email"].values():
	print len(config["Email"].values())

DEBUG = config["Debug"].getboolean("debug")
if DEBUG:
	print DEBUG

DB_HOST, DB_USER, DB_PASS, DB_NAME = config["Database Settings"].values()
print DB_NAME
print DB_PASS
print DB_USER
print DB_HOST