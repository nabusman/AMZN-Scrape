import dbdo, io, os, helpers

def create_report(num_products=500):
	"""Creates and mails a report of the top num_products=500 fastest moving 
	products
	Final list format: [slope, asin, product_name, manufacturer_name, 
	category_name, product_url]"""
	cur = dbdo.get_fastest_moving_products(num_products)
	numlines = int(cur.rowcount)
	filename = 'Top ' + str(num_products) + ' Fastest Moving Products - ' \
	+ str(datetime.date.today())
	file_path = os.path.join('reports/', filename)
	with io.open(file_path, 'w') as fmpfile:
		for i in range(numlines):
			line = cur.fetchone()
			fmpfile.write(unicode(line[0]).strip() + '|' + \
				unicode(line[1]).strip() + '|' + unicode(line[2]).strip() \
				+ '|' + unicode(line[3]).strip() + '|' + \
				unicode(line[4]).strip() + '|' + unicode(line[5]).strip() \
				+ '|' + "\n")
	return file_path

def run_analytics():
	"""Runs analytics on the rank data in the database and emails a list of the 
	top 500 fastest rising products
	1. Pull all asins and calculate slope
	2. update the value for the slope in products table
	3. when done updating all slopes, return asin's sorted dsc by slope...
	4. fetch top 500 and email them
	"""
	category_results = dbdo.get_all_category_urls()
	for category_results_line in category_results:
		category_url = category_results_line[0]
		print 'analyzing category: ' + category_url
		asin_results = dbdo.get_category_scrape_date(category_url)
		if len(asin_results) < 2:
			continue
		asin_results = dbdo.get_distinct_asins(category_url)
		if len(asin_results) == 0:
			continue
		for asin_results_line in asin_results:
			asin = asin_results_line[0]
			slope = calc_slope(asin, category_url)
			if slope is None:
				continue
			results = dbdo.get_slopes(asin, category_url)
			if results == ():
				print slope
				print category_url
				print asin
				dbdo.set_slope(slope, category_url, asin)
			else:
				dbdo.update_slope(slope, category_url, asin)
	return True


def calc_slope(asin, category_url):
	"""Calculates the slope given asin and category_url
	1. create a list of lists, with each point for each asin [[x,y], [x,y]]: x 
	is days (starting from 0); y is rank
	2. given the list calculate the following variables: num_of_points, 
	sum_of_x, sum_of_y, sum_of_xy, sum_of_x_squared
	3. if num_of_points < 3: skip the asin
	4. calculate the slope of the line: ((num_of_points * sum_of_xy) - 
		(sum_of_x * sum_of_y)) / ((num_of_points * sum_of_x_squared) - 
		sum_of_x^2)
	"""
	datapoints = []
	results = dbdo.get_scrape_date_rank(category_url, asin)
	if len(results) == 0 or len(results) == 1:
		return None
	zero_date = results[0][0]
	for line in results:
		datapoints.append([helpers.day_delta(line[0], zero_date), int(line[1])])
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
		slope = ((num_of_points * sum_of_xy) - (sum_of_x * sum_of_y)) / \
		((num_of_points * sum_of_x_squared) - (sum_of_x * sum_of_x))
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
	"""Calculates the highest rank scraped on the latest run and then compares 
	that with older scrapes if a higher rank has been scraped previously that 
	means the lastest run was incomplete and returns the highest rank of the 
	latest run, if the category was completely scraped returns 1"""
	#Calc last scrape date
	scrape_date_results = dbdo.get_last_scrape_date(category_url)
	if scrape_date_results == ():
		return 1
	last_scrape = scrape_date_results[0][0]
	#Calc highest rank scraped in last scrape
	highest_rank_results = dbdo.get_highest_rank(category_url, last_scrape)
	if highest_rank_results == ():
		return 1
	highest_rank = highest_rank_results[0][0]
	#Calc max rank scraped ever
	max_rank_results = dbdo.get_highest_rank(category_url)
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
	results = [['http://www.amazon.com/s/ref=sr_hi_1?rh=n%3A3760901&ie=UTF8']]
	#Scraping only Health & Personal category_results... uncomment line below to scrape all
	#results = dbdo.get_all_category_urls()
	for line in results:
		category_url = line[0]
		start_rank = last_rank_scraped(category_url)
		#Check if last scrape attempt was completed
		if start_rank != 1:
			categories_to_scrape.append([category_url, start_rank])
			continue
		#Add to queue categories that haven't been scraped or are older than days
		scrape_date_results = dbdo.get_last_scrape_date(category_url)
		if scrape_date_results == ():
			categories_to_scrape.append([category_url, start_rank])
			continue
		last_scrape = scrape_date_results[0][0]
		if helpers.day_delta(last_scrape) < -days:
			categories_to_scrape.append([category_url, start_rank])
	return categories_to_scrape