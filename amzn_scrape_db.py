#!/usr/bin/env python2.7
import re, mechanize, sys, io, os, traceback, lxml.html, datetime, urllib2, signal, telnetlib, time, smtplib, fcntl
import MySQLdb as mdb

#Ensure only one instance is running
fh = open(os.path.realpath(__file__),'r')
try:
	fcntl.flock(fh,fcntl.LOCK_EX|fcntl.LOCK_NB)
except:
	print "Already running, exiting..."
	os._exit(0)

#Change working directory to current directory
if os.path.dirname(__file__) == os.path.basename(__file__):
	os.chdir(os.path.dirname(__file__))

#Analyze mode - only analyzes, does not scrape
if '--analyze' in sys.argv or '-a' in sys.argv:
	print "ANALYZE!!!"
	ANALYZE = True
else:
	ANALYZE = False

#Debug Info
DEBUG = True

#DB Constants
DB_HOST = 'localhost'
DB_USER = 'user'
DB_PASS = 'password'
DB_NAME = 'name'

#Create Reports Directory
directory = os.path.join('reports/')
if not os.path.exists(directory):
	os.makedirs(directory)

#Regex Compile
regex_nextbutton = re.compile(r'Next.*')
regex_dollarsign = re.compile(r'\$')
regex_by = re.compile(r'by')
regex_comma = re.compile(r',')


#-- Rank Page Functions --#
def process_category(rank_page_url, start_rank = 1, max_rank = 9600):
	"""Scrapes all the data in the given category url"""
	try:
	
		#Set basic variables and hit next to reach start_rank page, if required
		rank_page_lxml = get_page(rank_page_url)
		category = extract_category(rank_page_lxml)
		rank_per_page = 24
		if start_rank >= max_rank:
			return False, category
		elif start_rank > rank_per_page:
			next_times = start_rank / rank_per_page
			print 'Hitting next ' + str(next_times) + ' times...'
			for times in range(next_times):
				previous_rank_page_lxml = rank_page_lxml
				rank_page_lxml = hit_next(rank_page_lxml)
				if rank_page_lxml == False:
					retries = 0
					while True:
						print "Error with reaching start page on rank_page_lxml: " + previous_rank_page_lxml.base_url
						log("Error with reaching start page on rank_page_lxml: " + previous_rank_page_lxml.base_url)
						if retries > 50:
							return False, category
						retries += 1
						print "Retrying..."
						rank_page_lxml = get_page(previous_rank_page_lxml.base_url)
						rank_page_lxml = hit_next(rank_page_lxml)
						if rank_page_lxml != False:
							retries = 0
							break
	
		#Start extracting
		retries = 0
		while True:
			try:
				ranks_names_urls = extract_ranks_names_urls(rank_page_lxml)
			except Exception, e:
				print "Error in extracting ranks, names, and urls from rank page: " + rank_page_lxml.base_url
				print e
				log(e)
				log(traceback.print_exc())
				raise
			
			if ranks_names_urls == []:
				print 'retries: ' + str(retries)
				if retries > 50:
					log("Error in extracting ranks names and urls from: " + rank_page_lxml.base_url)
					raise Exception("Error in extracting ranks names and urls from: " + rank_page_lxml.base_url)
				retries += 1
				print "Rank name url list is empty! Retrying..."
				rank_page_lxml = get_page(rank_page_lxml.base_url)
				continue
			else:
				retries = 0
			
			for rank_name_url in ranks_names_urls:
				rank = rank_name_url[0]
				name = rank_name_url[1]
				url = rank_name_url[2]
				asin = extract_asin(url)
				manufacturer, price, sold_by = process_product_page(url)
				todays_date = str(datetime.date.today())
				#[category_url, rank, asin, product_name, manufacturer_name, price, selling_status, product_url, scrape_date]
				save_product([rank_page_url, rank, asin, name, manufacturer, price, sold_by, url, todays_date])
	
			try:
				last_rank = ranks_names_urls[-1][0]
				if last_rank >= max_rank:
					break
			except Exception, e:
				print 'Error in finding last rank'
				print ranks_names_urls
				log(rank_page_lxml.text_content())
				raise

			rank_page_lxml = hit_next(rank_page_lxml)
			if rank_page_lxml == False or None:
				return True, category
		
		return True, category
	except Exception, e:
		print 'Error downloading category: ' + rank_page_url
		print e
		print traceback.print_exc()
		return False, category


def extract_ranks_names_urls(rank_page_lxml):
	"""Extracts the ranks, names, and urls given rank page html in unicode"""
	if DEBUG:
		print "[DEBUG] Extracting ranks, names, urls from rank page: " + rank_page_lxml.base_url
	ranks_names_urls = []
	classes = ['result firstRow product', 'result product', 'result lastRow product']
	
	for name in classes:
		results = rank_page_lxml.find_class(name)
		for result in results:
			
			try:
				rank = int(re.sub("\.", '', result.find_class('number')[0].text_content()))
			except:
				log('Rank error on page')
				rank = 'Error'
			
			try:
				title_class = result.find_class('title')[1]
				name = unicode(title_class.text_content()).strip()
				name = remove_unsafe_chars(name)
			except:
				log('Name error on page')
				name = 'Error'
			
			try:
				url = title_class.iterlinks().next()[2].strip()
			except:
				log('Title error on page')
				url = 'Error'
			
			ranks_names_urls.append([rank, name, url])
	
	return ranks_names_urls


def extract_category(rank_page_lxml):
	"""Extracts the category given rank page html in unicode"""
	if DEBUG:
		print "[DEBUG] Extracting category from rank page: " + rank_page_lxml.base_url
	try:
		category = rank_page_lxml.cssselect('h1#breadCrumb')[0].text_content().strip()
		category = category.split("\n")[-1].strip()
	except:
		log('Category error on page')
		category = 'Error'
	if category == 'Error':
		con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
		with con:
			cur = con.cursor()
			cur.execute("select category_name from categories where category_url='%s'" % str(rank_page_lxml.base_url))
			results = cur.fetchall()
			category = results[0][0]
	return category


#-- Product Page Functions --#
def process_product_page(product_url):
	"""Scrapes and returns all the data in the given product url"""
	if DEBUG:
		print "[DEBUG] Extracting manufacturer, price, and sold by from product page: " + product_url
	product_page_lxml = get_page(product_url)
	if product_page_lxml == False:
		print "Scrape of product page failed!!! URL: " + product_url
		log("Scrape of product page failed!!! URL: " + product_url)
		return 'Error', 'Error', 'Error'
	try:
		manufacturer = extract_manufacturer(product_page_lxml)
		price = extract_price(product_page_lxml)
		sold_by = extract_sold_by(product_page_lxml)
	except Exception, e:
		print e
		print traceback.print_exc()
		log('Error with url: ' + product_url)
		raise
	
	return manufacturer, price, sold_by


def extract_sold_by(product_page_lxml):
	"""Extracts who the product is sold by given a product page html in unicode"""
	sold_by = u'Not Sold by Amazon'
	
	try:
		results = product_page_lxml.cssselect('div.buying')
		for search_result in results:
			search_result = search_result.text_content()
			if 'Fulfilled by Amazon' in search_result:
				sold_by = u'Fulfilled by Amazon'
				break
			elif 'Ships from and sold by Amazon.com' in search_result:
				sold_by = u'Sold by Amazon'
				break
			elif 'Ships from and sold by Amazon Digital Services' in search_result:
				sold_by = u'Sold by Amazon'
				break
			else:
				continue
	except:
		log('Sold by error on page')
		sold_by = 'Error'
	
	return sold_by


def extract_price(product_page_lxml):
	"""Extracts the price of object given a product page html in unicode"""
	try:
		price = product_page_lxml.cssselect('span#actualPriceValue')
		if price != [] and "$" in price[0].text_content():
			price = re.sub(regex_dollarsign, '', price[0].text_content())
			if ',' in price:
				price = re.sub(regex_comma, '', price)
			price = float(price)
		else:
			price = 0.0
	except:
		log('Price error on page')
		price = 'Error'
	
	return price


def extract_manufacturer(product_page_lxml):
	"""Extracts the manufacturer given a product page html in unicode"""
	manufacturer = 'Error'
	try:
		results = product_page_lxml.cssselect('div.buying')
		for result in results:
			test = result.cssselect('h1.parseasinTitle')
			if test != []:
				manufacturer = re.sub(regex_by, '', result.cssselect('span')[1].text_content()).strip()
				break
	except:
		log('Manufacturer error on page')
		manufacturer = 'Error'
	return remove_unsafe_chars(manufacturer)


#-- Database Functions --#
def create_report(num_products=500):
	"""Creates and mails a report of the top num_products=500 fastest moving products
	Final list format: [slope, asin, product_name, manufacturer_name, category_name, product_url]"""
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
		cur.execute("select slopes.slope, slopes.asin, products.product_name, products.manufacturer_name, \
		categories.category_name, products.product_url from slopes inner join products on \
		slopes.asin=products.asin inner join categories on slopes.category_url=categories.category_url \
		order by slope asc limit %s" % num_products)
		numlines = int(cur.rowcount)
		filename = 'Top ' + str(num_products) + ' Fastest Moving Products - ' + str(datetime.date.today())
		file_path = os.path.join('reports/', filename)
		with io.open(file_path, 'w') as fmpfile:
			for i in range(numlines):
				line = cur.fetchone()
				fmpfile.write(unicode(line[0]).strip() + '|' + unicode(line[1]).strip() + '|' + \
				unicode(line[2]).strip() + '|' + unicode(line[3]).strip() + '|' + unicode(line[4]).strip() \
				+ '|' + unicode(line[5]).strip() + '|' + "\n")
		return file_path

def run_analytics():
	"""Runs analytics on the rank data in the database and emails a list of the top 500 fastest rising products
	1. Pull all asins and calculate slope
	2. update the value for the slope in products table
	3. when done updating all slopes, return asin's sorted dsc by slope...
	4. fetch top 500 and email them
	"""
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
		cur.execute("select category_url from categories")
		category_results = cur.fetchall()
		for category_results_line in category_results:
			category_url = category_results_line[0]
			print 'analyzing category: ' + category_url
			cur.execute("select distinct scrape_date from rankings where category_url='%s'" % category_url)
			asin_results = cur.fetchall()
			if len(asin_results) < 2:
				continue
			cur.execute("select distinct asin from rankings where category_url='%s'" % category_url)
			asin_results = cur.fetchall()
			if len(asin_results) == 0:
				continue
			for asin_results_line in asin_results:
				asin = asin_results_line[0]
				slope = calc_slope(asin, category_url)
				if slope is None:
					continue
				cur.execute("select * from slopes where asin='%s' and category_url='%s'" % (asin, category_url))
				results = cur.fetchall()
				if results == ():
					print slope
					print category_url
					print asin
					cur.execute("insert into slopes(slope, category_url, asin) values(%s, '%s', '%s')" % (slope, category_url, asin))
					con.commit()
				else:
					cur.execute("update slopes set slope=%s where category_url='%s' and asin='%s'" % (slope, category_url, asin))
					con.commit()
	return True


def calc_slope(asin, category_url):
	"""Calculates the slope given asin and category_url
	1. create a list of lists, with each point for each asin [[x,y], [x,y]]: x is days (starting from 0); y is rank
	2. given the list calculate the following variables: num_of_points, sum_of_x, sum_of_y, sum_of_xy, sum_of_x_squared
	3. if num_of_points < 3: skip the asin
	4. calculate the slope of the line: ((num_of_points * sum_of_xy) - (sum_of_x * sum_of_y)) / ((num_of_points * sum_of_x_squared) - sum_of_x^2)
	"""
	datapoints = []
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
		cur.execute("select scrape_date, rank from rankings where category_url='%s' and \
		asin='%s' order by scrape_date asc" % (category_url, asin))
		results = cur.fetchall()
		if len(results) == 0 or len(results) == 1:
			return None
		zero_date = results[0][0]
		for line in results:
			datapoints.append([day_delta(line[0], zero_date), int(line[1])])
		num_of_points = len(datapoints)
		sum_of_x = 0.0
		sum_of_y = 0.0
		sum_of_xy = 0.0
		sum_of_x_squared = 0.0
		for datum in datapoints:
			sum_of_x += datum[0]
			sum_of_y += datum[1]
			sum_of_xy += (datum[0] * datum[1])
			sum_of_x_squared += (datum[0] * datum[0])
		try:
			slope = ((num_of_points * sum_of_xy) - (sum_of_x * sum_of_y)) / ((num_of_points * sum_of_x_squared) - (sum_of_x * sum_of_x))
		except Exception, e:
			print datapoints
			print num_of_points
			print sum_of_xy
			print sum_of_x
			print sum_of_y
			print sum_of_x_squared
			raise
		return slope


def last_rank_scraped(category_url):
	"""Calculates the highest rank scraped on the latest run and then compares that with older scrapes
	if a higher rank has been scraped previously that means the lastest run was incomplete and returns
	the highest rank of the latest run, if the category was completely scraped returns 1"""
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
		#Calc last scrape date
		cur.execute("select scrape_date from rankings where category_url='%s' \
		order by scrape_date desc limit 1" % category_url)
		scrape_date_results = cur.fetchall()
		if scrape_date_results == ():
			return 1
		last_scrape = scrape_date_results[0][0]
		#Calc highest rank scraped in last scrape
		cur.execute("select rank from rankings where category_url='%s' and \
		scrape_date='%s' order by rank desc limit 1" % (category_url, last_scrape))
		highest_rank_results = cur.fetchall()
		if highest_rank_results == ():
			return 1
		highest_rank = highest_rank_results[0][0]
		#Calc max rank scraped ever
		cur.execute("select rank from rankings where category_url='%s' order \
		by rank desc limit 1" % category_url)
		max_rank_results = cur.fetchall()
		if max_rank_results == ():
			return 1
		max_rank = max_rank_results[0][0]
		if highest_rank != max_rank:
			return highest_rank
		else:
			return 1


def calc_categories_to_scrape(days=5):
	"""Returns an array of categories to scrape, which are older than 5 days"""
	categories_to_scrape = []
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
		cur.execute("select category_url from categories")
		results = cur.fetchall()
		for line in results:
			category_url = line[0]
			start_rank = last_rank_scraped(category_url)
			#Check if last scrape attempt was completed
			if start_rank != 1:
				categories_to_scrape.append([category_url, start_rank])
				continue
			#Add to queue categories that haven't been scraped or are older than days
			cur.execute("select scrape_date from rankings where category_url='%s' \
			order by scrape_date desc limit 1" % category_url)
			scrape_date_results = cur.fetchall()
			if scrape_date_results == ():
				categories_to_scrape.append([category_url, start_rank])
				continue
			last_scrape = scrape_date_results[0][0]
			if day_delta(last_scrape) < -days:
				categories_to_scrape.append([category_url, start_rank])
	return categories_to_scrape
			

def save_product(product_data):
	"""Saves data to database, product_data in format [category_url, rank, asin, product_name, manufacturer_name, price, selling_status, product_url, scrape_date]"""
	category_url, rank, asin, product_name, manufacturer_name, price, selling_status, product_url, scrape_date = product_data
	print category_url
	print rank
	print product_name
	print manufacturer_name
	print ''
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
	
		#Check if manufacturer exists, if not create it
		cur.execute("SELECT * FROM manufacturers WHERE manufacturer_name='%s'" % manufacturer_name)
		results = cur.fetchall()
		if results == ():
			cur.execute("INSERT INTO manufacturers(manufacturer_name) VALUES('%s')" % (manufacturer_name))
			con.commit()
	
		#Check to see if product exists, if not create it
		cur.execute("SELECT * FROM products WHERE asin='%s'" % asin)
		results = cur.fetchall()
		if results == ():
			cur.execute("INSERT INTO products(asin, product_name, product_url, manufacturer_name) VALUES('%s', '%s', '%s', '%s')" % (asin, product_name, product_url, manufacturer_name))
			con.commit()

		#Check to see if category exists, if not create it
		cur.execute("SELECT * FROM categories WHERE category_url='%s'" % category_url)
		results = cur.fetchall()
		if results == ():
			category_name = extract_category(category_url)
			cur.execute("INSERT INTO categories(category_url, category_name) VALUES('%s', '%s')" % (category_url, category_name))
			con.commit()
	
		#Create the ranking if it doesnt exist
		cur.execute("SELECT * FROM rankings WHERE asin='%s' AND scrape_date='%s' AND category_url='%s'" % (asin, scrape_date, category_url))
		results = cur.fetchall()
		if results == () and price:
			cur.execute("INSERT INTO rankings(scrape_date, price, rank, asin, category_url, selling_status) VALUES('%s', '%s', '%s', '%s', '%s', '%s')" % (scrape_date, price, rank, asin, category_url, selling_status))
			con.commit()
		elif results == ():
			cur.execute("INSERT INTO rankings(scrape_date, rank, asin, category_url, selling_status) VALUES('%s', '%s', '%s', '%s', '%s')" % (scrape_date, rank, asin, category_url, selling_status))
			con.commit()
		return True


#-- Helper Functions --#
def day_delta(date_str, compared_date_str = "Today"):
	"""Takes a date in str and returns date - today (or compared date if given)
	(ie negative if compared_date is after date, positive if compared_date if before date)
	date and compared date are to be provided in the format YYYY-MM-DD as a string"""
	date_str = str(date_str)
	compared_date_str = str(compared_date_str)
	if compared_date_str == "Today":
		compared_date_str = str(datetime.date.today())
	date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
	compared_date = datetime.datetime.strptime(compared_date_str, '%Y-%m-%d').date()
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


def sendMail(message, subject='Status message from AMZN_Scrape', from_address='amzn_scrape@domain.com', to_address='your.name@domain.com'):
	"""Sends email"""
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
	if DEBUG:
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
	br.addheaders = [("User-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:9.0.1) Gecko/20100101 Firefox/9.0.1")]
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
			logfile.write(unicode(datetime.datetime.now()) + '|' + unicode(mem_check()) + '|' + message + '|' + '\n')
		else:
			logfile.write(unicode(datetime.datetime.now()) + '|' + message + '\n')


#-- Main --#
if __name__=="__main__" and not ANALYZE:
	failed_categories = []
	try:
		categories_to_scrape = calc_categories_to_scrape()
		print 'Scraping Queue:'
		for line in categories_to_scrape:
			category_url = line[0]
			print category_url
		for line in categories_to_scrape:
			category_url = line[0]
			start_rank = line[1]
			status, category = process_category(category_url, start_rank)
			if status:
				print category + ' successfully scraped!'
				sendMail(category + ' successfully scraped!')
			else:
				print category + ' scrape failed!'
				sendMail(category + ' scrape failed!')
				failed_categories.append(category_url)
		run_analytics()
		create_report()
	except Exception, e:
		print "AMZN_Scrape Crashed!!!"
		email_message = "At " + unicode(datetime.datetime.now()) + " AMZN_Scrape crashed:" + '\n' + unicode(e) + '\n' + unicode(traceback.print_exc()) + '\n'
		sendMail(email_message, "AMZN_Scrape Crashed!!!")
		log("AMZN_Scrape Crashed!!!")
		log(e)
		log(traceback.print_exc())
		print e
		print traceback.print_exc()
elif __name__=="__main__" and ANALYZE:
	run_analytics()
	create_report()

