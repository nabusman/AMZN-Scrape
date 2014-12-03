import datetime, re, os, configparser, traceback, signal, io, lxml.html, \
mechanize
import MySQLdb as mdb


def init_config():
	"""Returns the configparser handler"""
	config = configparser.ConfigParser()
	if os.path.exists("settings.config"):
		config.read("settings.config")
	else:
		config.read("settings-default.config")
	return config


def get_database_config():
	"""Returns the database configuration from settings.config in the form of
	DB_HOST, DB_USER, DB_PASS, DB_NAME
	"""
	return init_config()["Database Settings"].values()


def get_email_config():
	"""Returns the email configurations in settings.config in the form
	from address, to address
	"""
	return init_config()["Email"].values()


def get_debug_config():
	"""Returns the status of debug flag in settings.config"""
	return init_config()["Debug"].getboolean("debug")


def day_delta(date_str, compared_date_str = "Today"):
	"""Takes a date in str and returns date - today (or compared date if given)
	(ie negative if compared_date is after date, positive if compared_date if 
	before date) date and compared date are to be provided in the format 
	YYYY-MM-DD as a string
	"""
	date_str = str(date_str)
	compared_date_str = str(compared_date_str)
	if compared_date_str == "Today":
		compared_date_str = str(datetime.date.today())
	date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
	compared_date = datetime.datetime.strptime(compared_date_str, \
		'%Y-%m-%d').date()
	delta = date - compared_date
	return delta.days


def remove_unsafe_chars(unsafe_str):
	"""Removes | and \n from string and makes it mysql ready"""
	safe_str = unsafe_str.strip()
	safe_str = re.sub(r'[^a-zA-Z0-9\s]', '', safe_str)
	safe_str = mdb.escape_string(safe_str)
	return unicode(safe_str)


def extract_asin(url):
	"""Takes a Amazon product url and returns the ASIN as a string"""
	is_asin = False
	for part in url.split("/"):
		if is_asin:
			return part
		if part == 'dp':
			is_asin = True
	return "Error"


def send_mail(message, subject='Status message from AMZN_Scrape'):
	"""Sends email"""
	email_config = get_email_config()
	if email_config and len(email_config) == 2:
		from_address, to_address = email_config
		sendmail_location = "/usr/sbin/sendmail"
		p = os.popen("%s -t" % sendmail_location, "w")
		p.write("From: %s\n" % from_address)
		p.write("To: %s\n" % to_address)
		p.write("Subject: %s\n" % subject)
		p.write("\n")
		p.write(message)
		status = p.close()


def get_page(url):
	"""Given a URL, returns an lxml doc at root"""
	if get_debug_config():
		print "[DEBUG] Downloading page: " + url
	timeout = 120
	while True:
		try:
			br = initialize_browser()
			signal.signal(signal.SIGALRM, signal_handler)
			signal.alarm(timeout)
			br.open(url)
			lxml_page = lxml.html.parse(br.response()).getroot()
			signal.alarm(0)  
			return lxml_page
		except Exception, e:
			print "Error with url: " + url
			log("Error with url: " + url)
			log(unicode(e))
			log(unicode(traceback.print_exc()))
			print e
			print traceback.print_exc()
			continue


def signal_handler(signum, frame):
	print 'Signal handler called with signal', signum
	raise Exception("get_page Timed Out!")


def hit_next(rank_page_lxml):
	"""Returns the next page (or False) given an lxml page"""
	try:
		links = rank_page_lxml.cssselect('a#pagnNextLink')
		if links == []:
			return False
		for link in links:
			next_page_url = 'http://www.amazon.com' + link.iterlinks().next()[2]
		return get_page(next_page_url)
	except Exception, e:
		print "Error with hitting next() on url: " + rank_page_lxml.base_url
		log("Error with hitting next() on url: " + rank_page_lxml.base_url)
		log(unicode(e))
		log(unicode(traceback.print_exc()))
		print e
		print traceback.print_exc()
		raise		


def initialize_browser():
	"""Returns a fully setup mechanize browser instance"""
	br = mechanize.Browser()
	br.addheaders = [("User-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X \
		10.9; rv:33.0) Gecko/20100101 Firefox/33.0")]
	return br


def mem_check():
	"""A quick memory usage check"""
	import resource
	size = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
	if size < 999999999:
		size /= 1000000
		return str(size) + ' MB'
	elif size < 999999999999:
		size /= 1000000000
		return str(size) + ' GB'
	else:
		return size


def log(message, memory_check = False):
	"""Logs to a local file log.txt"""
	with io.open('log.txt', 'a') as logfile:
		if memory_check:
			logfile.write(unicode(datetime.datetime.now()) + '|' + \
				unicode(mem_check()) + '|' + message + '|' + '\n')
		else:
			logfile.write(unicode(datetime.datetime.now()) + '|' + message + '\n')