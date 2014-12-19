import helpers
import MySQLdb as mdb

#DB Constants
DB_HOST, DB_USER, DB_PASS, DB_NAME = helpers.get_database_config()


def execute_sql(sql_string):
	"""Executes SQL for the given database and returns cursor object"""
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
		cur.execute(sql_string)
		return cur


def get_category_name(category_url):
	"""Gets the category name given category url from the database"""
	return execute_sql("SELECT category_name FROM categories WHERE \
				category_url='%s'" % str(category_url)).fetchall()


def get_fastest_moving_products(num_products):
	"""Gets the fastest moving products by slope"""
	return execute_sql("SELECT slopes.slope, slopes.asin, products.product_name,\
		products.manufacturer_name, categories.category_name, \
		products.product_url FROM slopes INNER JOIN products ON \
		slopes.asin=products.asin INNER JOIN categories ON \
		slopes.category_url=categories.category_url ORDER BY slope ASC \
		LIMIT %s" % num_products)


def get_all_category_urls():
	"""Gets all category urls for all categories"""
	return execute_sql("SELECT category_url FROM categories").fetchall()


def get_category_scrape_date(category_url):
	"""Gets latest scrape date given category url"""
	return execute_sql("SELECT DISTINCT scrape_date FROM rankings WHERE \
		category_url='%s'" % category_url).fetchall()


def get_distinct_asins(category_url):
	"""Returns distinct asins given a category_url"""
	return execute_sql("SELECT DISTINCT asin FROM rankings WHERE \
		category_url='%s'" % category_url).fetchall()


def get_slopes(asin, category_url):
	"""Returns everything in slopes given asin and category_url"""
	return execute_sql("SELECT * FROM slopes WHERE asin='%s' AND \
		category_url='%s'" % (asin, category_url)).fetchall()


def set_slope(slope, category_url, asin):
	"""Adds a new slope with the given data"""
	execute_sql("INSERT INTO slopes(slope, category_url, asin) \
		values(%s, '%s', '%s')" % (slope, category_url, asin))


def update_slope(slope, category_url, asin):
	"""Updates slope with given data"""
	execute_sql("UPDATE slopes SET slope=%s WHERE \
		category_url='%s' and asin='%s'" % (slope, \
		category_url, asin))


def get_scrape_date_rank(category_url, asin):
	"""Returns the scrape date and rank ordered by scrape date ascending
	for the given category url and asin
	"""
	return execute_sql("SELECT scrape_date, rank FROM rankings WHERE \
			category_url='%s' AND asin='%s' ORDER BY scrape_date ASC" % \
			(category_url, asin)).fetchall()


def get_last_scrape_date(category_url):
	"""Returns the last scraped date for given category url"""
	return execute_sql("SELECT scrape_date FROM rankings WHERE \
		category_url='%s' ORDER BY scrape_date DESC LIMIT 1" % \
		category_url).fetchall()


def get_highest_rank(category_url, scrape_date = "ALLTIME"):
	"""Returns the higest ranked scraped for given url and on given date or the
	highest rank ever scraped
	"""
	if scrape_date == "ALLTIME":
		return execute_sql("SELECT rank from rankings where category_url='%s' \
			order by rank desc limit 1" % category_url).fetchall()		
	else:
		return execute_sql("SELECT rank FROM rankings WHERE category_url='%s' \
			AND scrape_date='%s' ORDER BY rank DESC LIMIT 1" % (category_url, \
			scrape_date)).fetchall()


def save_product(product_data):
	"""Saves data to database, product_data in format [category_url, rank, \
	asin, product_name, manufacturer_name, price, selling_status, product_url, \
	scrape_date]"""
	category_url, rank, asin, product_name, manufacturer_name, price, \
	selling_status, product_url, scrape_date = product_data
	print category_url
	print rank
	print product_name
	print manufacturer_name
	print ''
	con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
	with con:
		cur = con.cursor()
	
		#Check if manufacturer exists, if not create it
		cur.execute("SELECT * FROM manufacturers WHERE manufacturer_name='%s'" \
			% manufacturer_name)
		results = cur.fetchall()
		if results == ():
			cur.execute("INSERT INTO manufacturers(manufacturer_name) \
				VALUES('%s')" % (manufacturer_name))
			con.commit()
	
		#Check to see if product exists, if not create it
		cur.execute("SELECT * FROM products WHERE asin='%s'" % asin)
		results = cur.fetchall()
		if results == ():
			cur.execute("INSERT INTO products(asin, product_name, product_url, \
				manufacturer_name) VALUES('%s', '%s', '%s', '%s')" % (asin, \
				product_name, product_url, manufacturer_name))
			con.commit()

		#Check to see if category exists, if not create it
		cur.execute("SELECT * FROM categories WHERE category_url='%s'" % \
			category_url)
		results = cur.fetchall()
		if results == ():
			category_name = extract_category(category_url)
			cur.execute("INSERT INTO categories(category_url, category_name) \
				VALUES('%s', '%s')" % (category_url, category_name))
			con.commit()
	
		#Create the ranking if it doesnt exist
		cur.execute("SELECT * FROM rankings WHERE asin='%s' AND \
			scrape_date='%s' AND category_url='%s'" % (asin, scrape_date, \
				category_url))
		results = cur.fetchall()
		if results == () and price:
			cur.execute("INSERT INTO rankings(scrape_date, price, rank, asin, \
				category_url, selling_status) VALUES('%s', '%s', '%s', '%s', \
				'%s', '%s')" % (scrape_date, price, rank, asin, category_url, \
				selling_status))
			con.commit()
		elif results == ():
			cur.execute("INSERT INTO rankings(scrape_date, rank, asin, \
				category_url, selling_status) VALUES('%s', '%s', '%s', '%s', \
				'%s')" % (scrape_date, rank, asin, category_url, \
				selling_status))
			con.commit()
		return True