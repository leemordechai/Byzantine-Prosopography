# -*- coding: utf-8 -*-
# to get this to display on the command prompt, 
#right click on window header -> properties and select font Consolas (for German and Greek)
import bs4 as bs 	# beautiful soup
import urllib.request

import sys	# to work with PyQt4, which is essentially a browser
from PyQt4.QtGui import QApplication	# to make applications
from PyQt4.QtCore import QUrl			# to read URLs
from PyQt4.QtWebKit import QWebPage 	# powerful for creating websites

class Client(QWebPage):
	def __init__(self, url):
		self.app = QApplication(sys.argv)
		QWebPage.__init__(self)			# to initialize
		self.loadFinished.connect(self.on_page_load)	# when load is finished, load page
		self.mainFrame().load(QUrl(url))
		self.app.exec_()

	def on_page_load(self):	
		self.app.quit()			# this means the app will run until the page loads, then quits

url = 'https://www.degruyter.com/view/PMBZ/PMBZ24682'

client_response = Client(url)
source = client_response.mainFrame().toHtml()
soup = bs.BeautifulSoup(source,'lxml')

for i in soup.find_all('h1', class_='entryTitle'):
	name = i.text

titles = soup.find_all('dt', class_="fieldName")
values = soup.find_all('dd', class_="fieldValue")

print('Name: ' + name)
for i in range(len(titles)):
	print(titles[i].text, values[i].text)

for code in soup.find_all('a'):
	if (len(code.text) > 1) and (code.text[0] == '#'): 
		iden = code.text[2:]			# this is the ID used on the front end
		urlpart = code.get('href')[15:]	# this is the url part to attach to the url for further crawling
		# URL = 'https://www.degruyter.com/view/PMBZ/PMBZ24682' <- urlpart fills the last digits here
		# print("Here: %s %s %s" % (iden, fake, fake-iden)) # this code revealed that urlpart and iden don't match

# to make this a viable scraper:
	# make class to keep all the individuals we've covered so far.
	# make a dictionary of these individuals
	# save all an individual's connections within a set
	# search through all those individuals and move them to a 'seen' set
	# even after all this, there would still be individuals who aren't connected to the broad mass