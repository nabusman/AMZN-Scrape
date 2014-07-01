import datetime
import MySQLdb as mdb

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

if __name__ == "__main__":
	run_analytics()
	create_report()
