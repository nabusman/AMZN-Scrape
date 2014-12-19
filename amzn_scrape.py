#!/usr/bin/env python2.7
import sys, os, traceback, datetime, fcntl
import helpers, analytics, rankpage

#Ensure only one instance is running
fh = open(os.path.realpath(__file__),'r')
try:
	fcntl.flock(fh,fcntl.LOCK_EX|fcntl.LOCK_NB)
except:
	print "Already running, exiting..."
	os._exit(0)

#Analyze mode - only analyzes, does not scrape
if '--analyze' in sys.argv or '-a' in sys.argv:
	print "ANALYZE ONLY!!!"
	ANALYZE = True
else:
	ANALYZE = False

#-- Main --#
if __name__=="__main__" and not ANALYZE:

	#Change working directory to current directory
	if os.path.dirname(__file__) == os.path.basename(__file__):
		os.chdir(os.path.dirname(__file__))

	#Create Reports Directory
	directory = os.path.join('reports/')
	if not os.path.exists(directory):
		os.makedirs(directory)

	failed_categories = []
	try:
		categories_to_scrape = analytics.calc_categories_to_scrape()
		print 'Scraping Queue:'
		for line in categories_to_scrape:
			category_url = line[0]
			print category_url
		for line in categories_to_scrape:
			category_url = line[0]
			start_rank = line[1]
			status, category = rankpage.process_category(category_url, start_rank)
			if status:
				print category + ' successfully scraped!'
				helpers.send_mail(category + ' successfully scraped!')
			else:
				print category + ' scrape failed!'
				helpers.send_mail(category + ' scrape failed!')
				failed_categories.append(category_url)
		analytics.run_analytics()
		analytics.create_report()
	except Exception, e:
		print "AMZN_Scrape Crashed!!!"
		email_message = "At " + unicode(datetime.datetime.now()) + " AMZN_Scrape \
		crashed:" + '\n' + unicode(e) + '\n' + unicode(traceback.print_exc()) + \
		'\n'
		helpers.send_mail(email_message, "AMZN_Scrape Crashed!!!")
		helpers.log("AMZN_Scrape Crashed!!!")
		helpers.log(e)
		helpers.log(traceback.print_exc())
		print e
		print traceback.print_exc()
elif __name__=="__main__" and ANALYZE:
	analytics.run_analytics()
	analytics.create_report()

