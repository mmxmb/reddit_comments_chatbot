# Estimate how much disk space is needed to store all comment data

from html.parser import HTMLParser
import re
import requests

class MyHTMLParser(HTMLParser):

	def handle_data(self, data):
		if (re.match('\d{1,3}(,\d{1,3})+', data)):
			size = int(''.join(data.split(',')))
			global total_size
			total_size += size

if __name__ == "__main__":
	total_size = 0
	reddit_comments_data = 'http://files.pushshift.io/reddit/comments/'
	html = requests.get(reddit_comments_data).content
	parser = MyHTMLParser()
	parser.feed(str(html))
	print('total size of bz2 compressed reddit comment data: {0:.2f}G'.format(total_size / (10**9)))
