import re, helpers, traceback

#Regex Compile
regex_dollarsign = re.compile(r'\$')
regex_by = re.compile(r'by')
regex_comma = re.compile(r',')

def process_product_page(product_url):
	"""Scrapes and returns all the data in the given product url"""
	if helpers.get_debug_config():
		print "[DEBUG] Extracting manufacturer, price, and sold by from \
		product page: " + product_url
	product_page_lxml = helpers.get_page(product_url)
	if product_page_lxml == False:
		print "Scrape of product page failed!!! URL: " + product_url
		helpers.log("Scrape of product page failed!!! URL: " + product_url)
		return 'Error', 'Error', 'Error'
	try:
		manufacturer = extract_manufacturer(product_page_lxml)
		price = extract_price(product_page_lxml)
		sold_by = extract_sold_by(product_page_lxml)
	except Exception, e:
		print e
		print traceback.print_exc()
		helpers.log('Error with url: ' + product_url)
		raise
	
	return manufacturer, price, sold_by


def extract_sold_by(product_page_lxml):
	"""Extracts who the product is sold by given a product page html in
	unicode"""
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
			elif 'Ships from and sold by Amazon Digital Services' in \
			search_result:
				sold_by = u'Sold by Amazon'
				break
			else:
				continue
	except:
		helpers.log('Sold by error on page')
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
		helpers.log('Price error on page')
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
				manufacturer = re.sub(regex_by, '', \
					result.cssselect('span')[1].text_content()).strip()
				break
	except:
		helpers.log('Manufacturer error on page')
		manufacturer = 'Error'
	return helpers.remove_unsafe_chars(manufacturer)