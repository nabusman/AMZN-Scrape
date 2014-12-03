import dbdo

url = "http://www.amazon.com/s/ref=sr_hi_1?rh=n%3A172282&ie=UTF8"
category = dbdo.get_category_name(str(url))[0][0]
print category