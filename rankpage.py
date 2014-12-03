import traceback, datetime, re, productpage, helpers, dbdo

def process_category(rank_page_url, start_rank = 1, max_rank = 9600):
	"""Scrapes all the data in the given category url"""
	try:
	
		#Set basic variables and hit next to reach start_rank page, if required
		rank_page_lxml = helpers.get_page(rank_page_url)
		category = extract_category(rank_page_lxml)
		rank_per_page = 24
		if start_rank >= max_rank:
			return False, category
		elif start_rank > rank_per_page:
			next_times = start_rank / rank_per_page
			print 'Hitting next ' + str(next_times) + ' times...'
			for times in range(next_times):
				previous_rank_page_lxml = rank_page_lxml
				rank_page_lxml = helpers.hit_next(rank_page_lxml)
				if rank_page_lxml == False:
					retries = 0
					while True:
						print "Error with reaching start page on \
						rank_page_lxml: " + previous_rank_page_lxml.base_url
						helpers.log("Error with reaching start page on rank_page_lxml:\
						 " + previous_rank_page_lxml.base_url)
						if retries > 50:
							return False, category
						retries += 1
						print "Retrying..."
						rank_page_lxml = \
						helpers.get_page(previous_rank_page_lxml.base_url)
						rank_page_lxml = helpers.hit_next(rank_page_lxml)
						if rank_page_lxml != False:
							retries = 0
							break
	
		#Start extracting
		retries = 0
		while True:
			try:
				ranks_names_urls = extract_ranks_names_urls(rank_page_lxml)
			except Exception, e:
				print "Error in extracting ranks, names, and urls from rank \
				page: " + rank_page_lxml.base_url
				print e
				helpers.log(e)
				helpers.log(traceback.print_exc())
				raise
			
			if ranks_names_urls == []:
				print 'retries: ' + str(retries)
				if retries > 50:
					helpers.log("Error in extracting ranks names and urls from: " + \
						rank_page_lxml.base_url)
					raise Exception("Error in extracting ranks names and urls \
						from: " + rank_page_lxml.base_url)
				retries += 1
				print "Rank name url list is empty! Retrying..."
				rank_page_lxml = helpers.get_page(rank_page_lxml.base_url)
				continue
			else:
				retries = 0
			
			for rank_name_url in ranks_names_urls:
				rank = rank_name_url[0]
				name = rank_name_url[1]
				url = rank_name_url[2]
				asin = helpers.extract_asin(url)
				manufacturer, price, sold_by = productpage.process_product_page(url)
				todays_date = str(datetime.date.today())
				#[category_url, rank, asin, product_name, manufacturer_name,
				# price, selling_status, product_url, scrape_date]
				dbdo.save_product([rank_page_url, rank, asin, name, manufacturer, \
					price, sold_by, url, todays_date])
	
			try:
				last_rank = ranks_names_urls[-1][0]
				if last_rank >= max_rank:
					break
			except Exception, e:
				print 'Error in finding last rank'
				print ranks_names_urls
				helpers.log(rank_page_lxml.text_content())
				raise

			rank_page_lxml = helpers.hit_next(rank_page_lxml)
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
	if helpers.get_debug_config():
		print "[DEBUG] Extracting ranks, names, urls from rank page: " + \
		rank_page_lxml.base_url
	ranks_names_urls = []
	product_list = rank_page_lxml.find_class('s-result-item')
	for product in product_list:
		try:
			prod_html = etree.tostring(product)
			rank_search = re.search(r'\<li id=\"result\_(\d+)\"', prod_html)
			rank = int(rank_search.group(1)) + 1
		except:
			helpers.log('Rank error on page')
			rank = 'Error'
	
		try:
			title = product.find_class('a-spacing-mini')[0]
			name = unicode(title.text_content()).strip()
			name = helpers.remove_unsafe_chars(name)
		except:
			helpers.log('Name error on page')
			name = 'Error'
		
		try:
			url = product.find_class('a-spacing-mini')[0].iterlinks().next()[2].strip()
		except:
			helpers.log('URL error on page')
			url = 'Error'
		
		ranks_names_urls.append([rank, name, url])
	return ranks_names_urls


def extract_category(rank_page_lxml):
	"""Extracts the category given rank page html in unicode"""
	if helpers.get_debug_config():
		print "[DEBUG] Extracting category from rank page: " + \
		 rank_page_lxml.base_url
	try:
		category = rank_page_lxml.cssselect('h2#s-result-count')
		category = category[0][0].text_content().strip()
	except:
		helpers.log('Category error on page')
		category = 'Error'
	if category == 'Error':
			category = dbdo.get_category_name(str(rank_page_lxml.base_url))[0][0]
	return category