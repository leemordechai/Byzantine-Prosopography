import pymysql, time, pydot

# sets up the massive file. 
# manual corrections: remove the extra line in #120418 (Michael 229); 
# 		rewrite the line for Konstantinos Choumnos (#159899, Konstantinos 20524)
def settingup(filename, cursor):
	runtime = time.time()
	f = open(filename, 'w')
	cursor.execute("select personKey, name, mdbcode, descName, \
			floruit, sexkey from person LIMIT 100000;")
	n = 0
	for row in cursor:
		temp = [str(x) for x in row]
		f.write("\t".join(temp) + "\n")
		n += 1
	f.close()
	print("Runtime is " + str(round(time.time()-runtime, 3)) + " seconds.")
	print("Received %s records." % n)	
	return 

# reads all (organized) entries from inputfile, organizes them in bigdict
def memoryload(inputfile):
	runtime = time.time()
	inp = open(inputfile, 'r')
	num_lines = sum(1 for line in open(inputfile))
	everyone = {}
	newline = inp.readline().strip().split('\t')
	while newline != [""]:
		p = Person(newline)
		everyone[p.id] = p
		newline = inp.readline().strip().split('\t')
		p.correctGender()	# running this replaces the gender code with a value
		p.correctTime()
		#p.findRelatives()	# this alone takes more than 30 seconds to run for entire DB
		#p.disp()
	print("Runtime is " + str(round(time.time()-runtime, 3)) + " seconds.")
	manualCorr(everyone)
	return everyone

def timeConv(t):	# to organize time
	arr = ['a', 'a', 'a', 'a']
	if len(t) == 4:
		arr[1] = t[0:2]
		arr[3] = t[0:2]
		arr = singleDig(t[2:4], arr)
		return arr
	if len(t) == 3:
		arr[1] = t[0:1]
		arr[3] = t[0:1]
		arr = singleDig(t[1:3], arr)
		return arr
	if len(t) == 7:	# 9xx-9xx
		arr[1] = t[0:1]
		arr[3] = t[0:1]
		arr1 = singleDig(t[1:3], arr)
		arr2 = singleDig(t[4:6], arr)
		arr[0] = arr1[0]
		arr[2] = arr2[2]
		return arr
	if len(t) == 9:	# 1xxx-1xxx
		arr[1] = t[0:1]
		arr[3] = t[0:1]
		arr1 = singleDig(t[2:4], arr)
		arr2 = singleDig(t[6:8], arr)
		arr[0] = arr1[0]
		arr[2] = arr2[2]
		return arr

def ethnicities(everyone): # updates everyone's ethnicities in memory
	conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
	cur = conn.cursor()
	cur.execute("SELECT p.personKey, name, mdbCode, fp.fpKey, fp.factoidKey, \
		ef.ethnicityKey, e.ethName FROM person p \
		INNER JOIN factoidperson fp ON p.personKey = fp.personKey \
		INNER JOIN ethnicityfactoid ef ON ef.factoidkey = fp.factoidkey \
		INNER JOIN ethnicity e ON ef.ethnicityKey = e.ethnicityKey;")
	for row in cur:
		temp = [str(x) for x in row]
		everyone[temp[0]].ethnic(temp[6]) # updates everyone's ethnicities
		#print(temp[6])

def relevantPeople(everyone): # returns all the relevant individuals (L10-E12)
	# this removes about 2,000 individuals who are mostly 12C.
	relev = {}
	n = 0
	for i, val in everyone.items():
		if val.floruit[1] == '11' or val.floruit[3] == '11' or \
			(val.floruit[0] == 'L' and val.floruit[1] == '10') or \
			(val.floruit[2] == 'E' and val.floruit[3] == '12'):
			relev[i] = val
			n += 1
	print('Selected only the relevant people (%s).' % n)
	return relev

# subfunction to organize time
def singleDig(s, arr): # s=2 digits, arr=newtime org, n=1 or 2 replacements
	if int(s) > 80:
		arr[0] = 'L'
		arr[2] = 'L'
	elif int(s) > 60:
		arr[0] = 'M'
		arr[2] = 'L'
	elif int(s) > 40:
		arr[0] = 'M'
		arr[2] = 'M'
	elif int(s) > 20:
		arr[0] = 'E'
		arr[2] = 'M'
	else:
		arr[0] = 'E'
		arr[2] = 'E'
	return arr

def manualCorr(everyone): # makes edits to the most basic version of the DB
	#these are all my manual corrections. 
	print("Took in a list of %s individuals." % len(everyone))
	del everyone['111596'] # removing empty entry, "TO BE DELETED"
	del everyone['113084'] # removing empty entry
	del everyone['159068'] # removing empty entry (former Ioannes Komnenos)
	del everyone['160715'] # removing empty entry (Ioannes, megas domestikos)
	del everyone['156509'] # removing empty
	del everyone['156525'] # removing empty
	del everyone['156565'] # removing empty
	del everyone['156722'] # removing empty
	del everyone['162260'] # removing empty
	del everyone['161694'] # removing empty
	del everyone['162402'] # removing empty
	del everyone['161820'] # removing entry for Michael Jeffreys
	everyone['153188'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['153268'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['153287'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['153796'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['155518'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	print("After deletions, %s individuals remain." % len(everyone))

# the overarching class of a Person. 
class Person():
	def __init__(self, line):
		self.id = line[0]
		self.name = [line[1], line[2], line[3]]
		self.floruit = line[4]
		self.gender = line[5]
		self.ethnicity = []

	def correctGender(self):
		# I decided to catalog all genderKey = 0 (unlisted) and 1 (unspecified) based on their
		# names and descriptions:
		#	- If a person has a female name, I cataloged as Female
		#	- In the case of some anonymous groups, I cataloged as Mixed (based on desc.)
		#	- Otherwise, I cataloged as Male
		#	- Some eunuchs were miscataloged, so I corrected them individually.
		genderKeys = {'2':"Male", '3':'Female', 
				'4':'Eunuch', '5': 'Mixed', '6':'Eunuch (probable)'}
		genderNames = ['Anonyma', 'Adelaide', 'Agnes', 'Anna', 'Anonymae', 'Cecilia', 'Constance',
						'Eirene', 'Eleanor', 'Eudokia', 'Florina', 'Isabella', 'Koronis', 'Maria',
						'Matilda', 'Matzo', 'Nadra', 'Theodora', 'Xene', 'Zoe']
		if self.gender in genderKeys: self.gender = genderKeys[self.gender]
		elif self.name[0] in genderNames: self.gender = 'Female' 
		elif self.name[0] == 'Anonymi':
			exceptions = ['162523', '161359', '162525', '161318', '161340', '161343',
						'161362', '161375', '161412', '161420', '161637', '161908', '161917']
			if self.id in exceptions: self.gender = 'Mixed'
		else: self.gender = 'Male'

		# corrections
		if self.id == '162040': self.gender = 'Eunuch'
		if self.id == '161208': self.gender = 'Eunuch'
		if self.id == '162123': self.gender = 'Eunuch'

	def correctTime(self):
		temp = self.floruit.replace(" ", "")
		orgTime = ["a", "a", "a", "a"]
		#dealing with centuries
		if 'XIII' in temp: 
			orgTime[3] = '13'
			temp = temp.replace('XIII',"")
		if 'XII' in temp:
			if orgTime[3] == '13': orgTime[1] = '12'
			else: orgTime[3] = '12'
			temp = temp.replace('XII', "")
		if 'XI' in temp:
			if orgTime[3] == '12': orgTime[1] = '11'
			else: orgTime[3] = '11'
			temp = temp.replace('XI', "")
		if 'X' in temp:
			if temp == 'IX/X': # exception
				orgTime[1] = '9'
				orgTime[3] = '10'
				temp = ''
			elif orgTime[3] == '11': orgTime[1] = '10'
			else: orgTime[3] = '10'
			temp = temp.replace('X', "")
		#cleaning the data, a lot of exceptions
		temp = temp.replace('c.', '')
		temp = temp.replace('?', '')
		if len(temp) == 1 and temp != '/':
			orgTime[0] = temp
			orgTime[2] = temp
		elif len(temp) == 3 and (temp[1] == '/' or temp[1] == '-'):
			orgTime[0] = temp[0:1]
			orgTime[2] = temp[2:3]
		elif orgTime[1] == 'a' and len(orgTime[3]) == 2: orgTime[1] = orgTime[3]
		elif temp == '/': temp = ''
		elif '11th' in temp: orgTime = ['E', '11', 'L', '11']
		elif temp == 'L/': orgTime[0] = 'L'
		elif temp == "1041/2": orgTime = ['M', "11", 'M', '11']
		elif len(temp) == 3 or len(temp) == 4 or len(temp) == 7 or len(temp) == 9: 
			orgTime = timeConv(temp)
		elif temp == 'mid': 
			orgTime[0] = 'M'
			orgTime[2] = 'M'
		else: print('Error: wrong format %s, %s, %s' % (self.id, self.floruit, orgTime))
		if (orgTime[1] == 'a' and orgTime[3] != 'a'): orgTime[1] = orgTime[3]			
		self.floruit = orgTime
		# currently (12.1.17) = 16 issues (all empty records)

	def findRelatives(self): # takes quite a bit to run
		self.relatives = []
		conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
		cur = conn.cursor()
		cur.execute("SELECT fp.personKey, f.engDesc, kt.gspecRelat, kt.gunspecRelat, \
			LEFT(RIGHT(f.engDesc,9),6) AS targ, fp2.personKey FROM factoid f \
			INNER JOIN factoidperson fp ON fp.factoidKey = f.factoidKey \
			INNER JOIN kinfactoid kf ON f.factoidKey = kf.factoidKey \
			INNER JOIN kinshiptype kt ON kf.kinKey = kt.kinKey \
			INNER JOIN factoidperson fp2 ON fp2.fpKey = LEFT(RIGHT(f.engDesc,9),6) \
    		WHERE fp.personKey = '%s' AND fp.fpTypeKey = 2 GROUP BY fp2.personKey;"
    		% self.id)
		
		for row in cur:
			temp = [str(x) for x in row]
			rela = [temp[2], temp[5], temp[3]]
			self.relatives.append(rela)
		#print(self.relatives)
			
	def disp(self): #displays a person in readable language
		print('ID: %s, Name: %s, Description: %s, Floruit: %s, Gender: %s'
			% (self.id, " ".join(self.name[0:2]), self.name[2], self.floruit, self.gender)) 

	def ethnic(self, val):
		if val not in self.ethnicity: self.ethnicity.append(val) 


def genderGraph(every, cenpart, century, narrow): #narrow = 0-2
	print('here')
	toDraw = {}
	graph = pydot.Dot(graph_type='graph') #can use 'digraph' for directed
	
	for i, val in every.items():
		if inTime(val, cenpart, century, narrow) == True:
			toDraw[i] = val

	links = []	# to keep single links
	for i, val in toDraw.items():
		val.findRelatives()
		for j in val.relatives:
			nEdge = (val.name[0] + " " + val.name[1], \
				every[j[1]].name[0] + " " + every[j[1]].name[1])
			rEdge = (every[j[1]].name[0] + " " + every[j[1]].name[1], \
				val.name[0] + " " + val.name[1])
			if nEdge not in links and rEdge not in links:
				links.append(nEdge)
				edge = pydot.Edge(val.name[0] + " " + val.name[1], \
					every[j[1]].name[0] + " " + every[j[1]].name[1])
				graph.add_edge(edge)

	graph.write_dot('rel_graph.dot')
	#graph.write_pdf('testgraph.pdf')


def inTime(p, cenpart, century, narrow): # returns True if person p lived in time
# cenpart, century, with narrow being the size of time-window. 
	if (p.floruit[0] == cenpart and p.floruit[1] == century) or \
		(p.floruit[2] == cenpart and p.floruit[3] == century) or \
		(p.floruit[0] == 'E' and p.floruit[2] == 'L' and cenpart == 'M' and \
			p.floruit[1] == century and p.floruit[3] == century):
		return True;
	elif narrow >= 1:
		temp = ['E', 'M', 'L', 'E']
		ind = temp.index(cenpart)
		cent = int(century)
		if cenpart == 'L': cent += 1
		return inTime(p, temp[ind+1], str(cent), narrow-1)
	return False


conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
cur = conn.cursor()
#settingup('basiclist.txt', cur)

everyone = memoryload('basiclist.txt')
ethnicities(everyone)	# loads all entries with their ethnicities
relev = relevantPeople(everyone) # removes all the non-relevant people from dict

genderGraph(everyone ,'L', '11', 0)	# needs path to C:\Program Files (x86)\Graphviz2.38\bin
# standard path is to: C:\Users\veredlee\AppData\Local\Programs\Python\Python36-32

# clean up
cur.close()
conn.close()


# to do:
# - make relations keep its records in a separate file and use 
#   it to load things faster.

