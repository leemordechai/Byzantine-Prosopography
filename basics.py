import pymysql, time, pydot, statistics, os
import subprocess #alternative to os
import networkx

# sets up the massive file of all individuals. 
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

# this function can be used to organize time based on the "M XI" system, obsolete.
def timeConv(t):	
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

# updates everyone's ethnicities in memory
def ethnicities(everyone): 
	conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
	cur = conn.cursor()
	cur.execute("SELECT p.personKey, name, mdbCode, fp.fpKey, fp.factoidKey, " + \
		"ef.ethnicityKey, e.ethName FROM person p \
		INNER JOIN factoidperson fp ON p.personKey = fp.personKey \
		INNER JOIN ethnicityfactoid ef ON ef.factoidkey = fp.factoidkey \
		INNER JOIN ethnicity e ON ef.ethnicityKey = e.ethnicityKey \
		WHERE fp.fpTypeKey=2;")
	for row in cur:
		temp = [str(x) for x in row]
		everyone[temp[0]].ethnic(temp[6]) # updates everyone's ethnicities

		
# returns all the relevant individuals (L10-E12)
# this removes about 2,000 individuals who are mostly 12C.
def relevantPeople(everyone): 
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

# an obsolete subfunction to organize time
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


# makes edits to (=cleans) the most basic version of the DB
# these are all my manual corrections, with brief explanations.
# it is best to add data (manually) here to keep track of changes to
# the database (whose dump file is not openly accessible as of 2017)
# for future official versions of it.
def manualCorr(everyone): 
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
	del everyone['156133'] # removing entry for Tzorbaneles 20101 (empty entry w/ failed relation)
	everyone['153188'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['153268'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['153287'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['153796'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['155518'].floruit = ['M', '11', 'M', '11'] #based on his surviving seal
	everyone['162310'].name[1] = '106' #typo, doesn't appear in online version, only in MySQL
	everyone['160873'].floruit = ['L', '12', 'E', '13'] # seems like a typo in MySQL (L/E XII)
	everyone['159311'].floruit = ['L', '12', 'E', '13'] # seems like a typo in MySQL (L XII/E XII)

	eunuchupdate = [	# adding in all the eunuchs from the main eunuch table
	['x27488C', 'Anonymus', 'x27488C', 'Uncle of Symeon the New Theologian', 'M/L X', '4'],
	['x29496', 'Petros', 'x29496', 'Originally supported Nikephoros 2', 'L X', '4'],
	['x20925', 'Basileios', 'x20925', 'Basil Lekapenos the Parakoimomenos', 'L X', '4'],
	['x20452', 'Anthes', 'x20452', 'Anthes Alyates', 'L X', '4'],
	['x24532', 'Leon', 'x24532', 'Worked with Basileios Lekapenos', 'L X', '4'],
	['x27488', 'Symeon', 'x27488', 'Symeon the New Theologian', 'L X / E XI', '4'],
	['x26352', 'Paulos', 'x26352', 'Paul of Xeropotamites', 'L X / E XI', '4'],
	['x23591', 'Knttis', 'x23591', 'Eunuch of Arab origins', 'L X', '4'],
	['x20609', 'Arsenios', 'x20609', 'Abbot who succeeded Symeon the New Theologian', 'L X / E XI', '4'],
	['x20818', 'Barnakumeon(?)', 'x20818', 'Eunuch bishop of Akmoneia', 'E XI', '4'],
	['x23163', 'Ioannes', 'x23163', 'Received several letters in X/XI C', 'E XI', '4'],
	['x26626', 'Philokales', 'x26626', 'Poor villager who became wealthy after buying the village commune', 'L X / E XI', '4'],
	['x26847', 'Romanos', 'x26847', 'Romanos the Bulgar; a Bulgarian prince', 'L X / E XI', '4'],
	['x27045', 'Sergios', 'x27045', 'Intimate chamberlain of Basileios 2', 'E XI', '4'],
	['x21876', 'Eustathios', 'x21876', 'Patriarch under Basileios 2', 'E XI', '4'],
	['x23370', 'Ioannes', 'x23370', 'Eunuch monk and oikonomos', 'E XI', '4'],
	['lm1', 'Sabas', 'l3000', 'Eunuch monk depicted in Nikephoros 3 manuscript', 'L XI', '4'],
	['lm2', 'Ioannes', 'l3000', 'Ioannes the Faster, abbot who became a saint', 'L XI', '4'],
	['lm3', 'Michael', 'l3000', 'Doctor/monk of Alexios 1', 'L XI / E XII', '4'],
	['lm4', 'Theodoulos', 'l3000', 'Archbishop of Thessaloniki', 'L XI / E XII', '4'],
	['lm5', 'Anonymus', 'l3000', 'Bishop of Kitros', 'L XI / E XII', '4']
	]
	for i in eunuchupdate:
		p = Person(i)
		p.correctTime()
		p.correctGender()
		everyone[i[0]] = p

	# corrections based on main eunuch list
	everyone['106911'].gender = 'Eunuch' # Theophylaktos of Ochrid's brother
	everyone['108103'].gender = 'Eunuch' # Orestes, Basil II's courtier
	everyone['107988'].gender = 'Eunuch' # Niketas of Pisidia, doux of Iberia
	everyone['107081'].gender = 'Eunuch' # Megas hetaireiarches under Constantine VIII
	everyone['108329'].gender = 'Eunuch' # Symeon, droungarios under Constantine VIII
	everyone['107955'].gender = 'Eunuch' # Nikephoros, protovestiarios under CON VIII
	everyone['107989'].gender = 'Eunuch' # Niketas of Mistheia, doux of Antioch
	everyone['107529'].gender = 'Eunuch' # Konstantinos Leichoudes, patriarch
	everyone['107285'].gender = 'Eunuch' # Ioannes the philosopher, courtier of Zoe
	everyone['106759'].gender = 'Eunuch' # Basileios, governor of Bulgaria
	everyone['107286'].gender = 'Eunuch' # Ioannes the logothetes
	everyone['106761'].gender = 'Eunuch' # Basileios, epi tou kanikleiou
	everyone['107552'].gender = 'Eunuch' # Konstantinos, protonotarios of the dromos
	everyone['107994'].gender = 'Eunuch' # Niketas Xylinites, logothetes of the dromos
	everyone['107727'].gender = 'Eunuch' # Manuel, droungarios of the vigla
	everyone['108369'].gender = 'Eunuch' # Theodoros, domestikos of the scholai
	everyone['162058'].gender = 'Eunuch' # Basileios the eunuch, nobellissimos 
	everyone['162059'].gender = 'Eunuch' # Konstantinos, once epi tes trapezes of Alexios I's father, now of Eirene Doukaina
	everyone['161466'].gender = 'Eunuch' # Ioannes, eunuch and mystikos
	everyone['x26847'].ethnic('Bulgarian') # Romanos the Bulgar

	moreeunuchs = [ # list B of eunuchs
	['x31996', 'Anonymus', 'l3001', 'Imperial eunuch killed by rebel', 'E XI', '4'],
	['x31997', 'Anonymus', 'l3002', 'Imperial eunuch who tries to poison Basileios 2', 'E XI', '4'],
	['lm6', 'Ioannes', 'l3001', 'Guardian of Lavra monastery under Konstantinos 9', 'M XI', '4'],
	['lm7', 'Anonymus', 'l3003', 'Official working with Philaretos and commander of Edessa', 'M / L XI', '4'],
	['lm8', 'Anonymus', 'l3004', 'A libidinous eunuch mentioned by Theophylaktos of Ochrid', 'L XI / E XII', '4'],
	['lm9', 'Anonymus', 'l3005', 'Bishop of Petra', 'L XI / E XII', '4'],
	['lm10', 'Anonymus', 'l3006', 'Bishop of Edessa in Macedonia', 'L XI / E XII', '4']
	]
	for i in moreeunuchs:
		p = Person(i)
		p.correctTime()
		p.correctGender()
		everyone[i[0]] = p
	# corrections based on eunuch list B
	everyone['108972'].gender = 'Eunuch' # Ioannes, praipositos and grammatikos of Michael Attaleiates
	everyone['120208'].gender = 'Eunuch' # Basileios, praipositos epi tou koitonos and chartophylax of sekreton of Myrelaion
	everyone['161483'].gender = 'Eunuch' # eunuch grammatikos who instructed Anna Komnene
	everyone['162086'].gender = 'Eunuch' # eunuch bedroom attendant of Alexios I
	everyone['162255'].gender = 'Eunuch' # incumbent of a see of Tyre and Sidon, later patriarch of Jerusalem
	everyone['162269'].gender = 'Eunuch' # eunuch responsible for a terrible tax audit near Ohrid
	
	print("After deletions and additions, %s individuals remain." % len(everyone))


# the overarching class of a Person. This is the basic data unit in these analyses.
class Person():
	def __init__(self, line):
		self.id = line[0]
		self.name = [line[1], line[2], line[3]]
		self.floruit = line[4]
		self.gender = line[5]
		self.ethnicity = []
		self.mdate = 1	# median of active events

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
						'Matilda', 'Matzo', 'Nadra', 'Theodora', 'Sophia', 'Xene', 'Zoe', 'Kale',
						'Bebdene', 'Semne', 'Anastaso', 'Aikaterine']
		if self.gender in genderKeys: self.gender = genderKeys[self.gender]
		if self.name[0] in genderNames: self.gender = 'Female' 
		elif self.name[0] == 'Anonymi':
			exceptions = ['162523', '161359', '162525', '161318', '161340', '161343',
						'161362', '161375', '161412', '161420', '161637', '161908', '161917']
			if self.id in exceptions: self.gender = 'Mixed'
		elif self.gender == '1' or self.gender == '0': self.gender = 'Male'

		# corrections
		if self.id == '162040': self.gender = 'Eunuch'
		if self.id == '161208': self.gender = 'Eunuch'
		if self.id == '162123': self.gender = 'Eunuch'
		if self.id == '106792': self.gender = 'Male'
		if self.id == '108065': self.gender = 'Male'
		if self.id == '107951': self.gender = 'Male' # Nikephoros 64 (Diogenes)

	# clean and standardize the floruit for a person
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
		if len(temp) == 1 and temp != '/' and temp != '-':
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
		if (orgTime[0] == 'a' and orgTime[2] != 'a'): orgTime[0] = orgTime[2]
		self.floruit = orgTime
		# currently (12.1.17) = 16 issues (all empty records)


	# determines the median date for an individual based on the factoids they are part of. 
	def medi(self):		
		conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
		cur = conn.cursor()
		cur.execute("SELECT nu.dates FROM factoidperson fp " + \
			"INNER JOIN narrativefactoid nf ON fp.factoidKey = nf.factoidKey " + \
    		"INNER JOIN narrativeunit nu ON nu.narrativeunitID = nf.narrativeUnitID " + \
    		"INNER JOIN factoid f ON fp.factoidKey = f.factoidKey " + \
    		"WHERE fp.personKey = '%s' AND fp.fpTypeKey=2 GROUP BY dates;" % self.id)
		dates = []
		for row in cur:
			temp = row[0].replace('*','') 
			#temp = temp.replace('c. ', '')
			if len(temp) >= 3 and temp[0:4].isnumeric(): dates.append(int(temp[0:4]))
		if len(dates) > 0: self.mdate = statistics.median(dates)
		#print(self.mdate, self.floruit)

	# finds all the relatives of a person. takes quite a bit to run
	def findRelatives(self): 
		self.relatives = []
		conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
		cur = conn.cursor()
		cur.execute("SELECT fp.personKey, f.engDesc, kt.gspecRelat, kt.gunspecRelat, " + \
			"LEFT(RIGHT(f.engDesc,9),6) AS targ, fp2.personKey FROM factoid f " + \
			"INNER JOIN factoidperson fp ON fp.factoidKey = f.factoidKey " + \
			"INNER JOIN kinfactoid kf ON f.factoidKey = kf.factoidKey " + \
			"INNER JOIN kinshiptype kt ON kf.kinKey = kt.kinKey " + \
			"INNER JOIN factoidperson fp2 ON fp2.fpKey = LEFT(RIGHT(f.engDesc,9),6) " + \
    		"WHERE fp.personKey = '%s' AND fp.fpTypeKey = 2 GROUP BY fp2.personKey;"
    		% self.id)
		
		for row in cur:
			temp = [str(x) for x in row]
			rela = [temp[2], temp[5], temp[3]]
			self.relatives.append(rela)
		#print(self.relatives)
			
	def disp(self): #displays a person in readable language
		print('ID: %s, Name: %s, Description: %s, Floruit: %s, Gender: %s'
			% (self.id, " ".join(self.name[0:2]), self.name[2], self.floruit, self.gender)) 

	# adds a new ethnicity to a person
	def ethnic(self, val):
		if val not in self.ethnicity: self.ethnicity.append(val) 




###########################################################
####### relationship network part functions ###############
###########################################################

#Former attempts to draw the gender graph (obsolete)
def genderGraph(every, cenpart, century, narrow): # narrow=0-2. obsolete 
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
				edge = pydot.Edge(val.name[0] + " " + val.name[1], \
					every[j[1]].name[0] + " " + every[j[1]].name[1])
				graph.add_edge(edge)
				links.append(nEdge)

	print(graph)
	graph.write_dot('rel_graph.dot')
	#graph.write_pdf('testgraph.pdf')

def inTime(p, cenpart, century, narrow): # obsolete
# returns True if person p lived in time cenpart, century, narrow = time-window size. 
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


# creates a graph of people (relatives) between both dates, marking gender visually.
def genderGraph(every, begin, end, vague=False, pre=100, post=100, 
	singles=False, documentary=False): 
# pre/post is num years after which will continue to draw
# vague=True includes people with only estimates (E/M XI) as peripheral nodes
# if vague=True, pre and post are ignored.
# positive booleans are things that should be in the graph
	
	graph = pydot.Dot(graph_type='graph') #can use 'digraph' for directed

	links = [] # to keep single links
	# manually break up these (false) corrections
	breakups = {('Konstantinos 8', 'Konstantinos 63'),	# prospective father in law
				('Michael 61', 'Eudokia 1'),  # Psellos is not Eudokia's uncle
				}
	
	# define blacklist
	print("Blacklisting from graph: foreigners", end="")
	if documentary == False: print(", documentary evidence", end="")
	if vague == False: print(", vague-date individuals", end="")
	print()
	blacklist = getSet(every, foreigners=True, document=not documentary)
	
	# draw network graph
	for i, val in every.items():
		name = val.name[0] + " " + val.name[1]
		if val.mdate >= begin and val.mdate <= end and name not in blacklist:
			val.findRelatives()
			if val.gender == 'Eunuch': n = pydot.Node(name, fontcolor='green')
			elif val.gender == 'Male': n = pydot.Node(name, fontcolor='blue')
			elif val.gender == 'Female': n = pydot.Node(name, fontcolor='red')
			else: n = pydot.Node(name, fontcolor='black')			
			if val.relatives != [] or singles == True: graph.add_node(n)
			for j in val.relatives: # j ~ ['father', '107925', 'Parent']
				targetname = every[j[1]].name[0] + " " + every[j[1]].name[1]
				if (every[j[1]].mdate > begin - pre and every[j[1]].mdate < end + post) \
					or vague == True: # fits user criteria
					nEdge = (name, targetname)
					rEdge = (targetname, name)
					#print(name, val.name[2])	# originally for blacklisting
					if nEdge not in links and rEdge not in links and \
						nEdge not in breakups and rEdge not in breakups:
						if every[j[1]].gender == 'Eunuch': t = pydot.Node(targetname, fontcolor='green')
						elif every[j[1]].gender == 'Male': t = pydot.Node(targetname, fontcolor='blue')
						elif every[j[1]].gender == 'Female': t = pydot.Node(targetname, fontcolor='red')
						else: t = pydot.Node(targetname, fontcolor='black')
						graph.add_node(t)
						attr_list = {} # can fill this to format
						if val.gender == 'Female' or every[j[1]].gender == 'Female':
							attr_list = {'color': 'red'}
						else: attr_list = {'color': 'black'} 
						edge = pydot.Edge(n, t, **attr_list)
						graph.add_edge(edge)
						links.append(nEdge)									

	print(graph) # just a reference to an object
	graph.write_dot('rel_graph.dot')
	
# saves the median dates for everyone in 'every' to a separate file
def calcMedian(every): # takes about 30 seconds, finds ~6,400 medians
	runtime = time.time()
	f = open('medians.txt', 'w')
	n = 0
	
	for i, val in every.items():
		val.medi()
		f.write(str(i) + "\t" + str(val.mdate) + "\n")
		if val.mdate > 1: n += 1
	f.close()
	print("Calculating median times took " + str(round(time.time()-runtime, 3)) \
		+ " seconds.")
	print("Calculated %s medians." % n)	

# loads the dates from the auxiliary file (faster than using calcMedian every time)
def loadMedian(every): 
	inp = open('medians.txt', 'r')
	n = 0
	newline = inp.readline().strip().split('\t')
	while newline != [""]:
		every[newline[0]].mdate = float(newline[1])
		if float(newline[1]) > 1: n += 1
		newline = inp.readline().strip().split('\t')
		
	print("Loaded %s medians." % n)


# this creates the relationships-gender graph. 
# who = all individuals     start and end refer to cutoff dates
# preint and postint refer to number of years before which and after which
#	connections would still appear (meant to capture parents and children in the graph)
def printGenGraph(who, start, end, minsize=5, preint=0, postint=20, \
	vag=False, sing=False, doc=False):
	# selecting vague=False or documentary=False removes many from list.
	# if vag=True, the preint and postint criteria do not matter
	# this creates the .dot file used later in this function
	

	genderGraph(who, start, end, vague=vag, pre=preint, post=postint, \
		singles=sing, documentary=doc) #true for vag, sin, doc = display them

	# this is just to provide output about the graph later
	temp = subprocess.Popen('ccomps -x rel_graph.dot | gvpr -c \
		"N[nNodes($G)<%s]{delete(0,$)}" | dot | gvpack | sfdp -Goverlap=prism \
		| gvmap -e | gc -a' % str(minsize), shell=True, stdout=subprocess.PIPE, \
		stderr=subprocess.STDOUT)
	for line in temp.stdout.readlines():
		i = line
		#print(i)# this is to see the actual input in the command line, often unnecessary
	
	temp = subprocess.Popen("""ccomps -x rel_graph.dot | gvpr -c \
		"N[nNodes($G)<5]{delete(0,$)}" | dot | gvpack | sfdp -Goverlap=prism \
		| gvmap -e | gvpr "BEGIN{int m,w,e = 0} \
		N[fontcolor=='blue']{m += 1} \
		N[fontcolor=='green']{e += 1} \
		N[fontcolor=='red']{w += 1} \
		END{printf('%d %d %d', m, w, e);}" 
		""", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	for line in temp.stdout.readlines():
		j = line		

	# this actually creates the requested graph-map
	subprocess.Popen("""ccomps -x rel_graph.dot | \
		gvpr -c "N[nNodes($G)<%s]{delete(0,$)}" | dot | gvpack | sfdp -Goverlap=prism \
		| gvmap -e | neato -n2 -Ecolor=#55555522 -Tpng > %s-%s_%splus.png""" %
		(str(minsize), str(start), str(end), str(minsize)),
		shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

	#formatted output for user
	print("Graph written to %s-%s_%splus.png." % (str(start), str(end), str(minsize)))
	i = i.decode("utf-8")
	ans = i[:len(i)-17].strip().split()
	print("This graph has %s nodes, %s edges, and %s connected components." % \
		(ans[0], ans[1], ans[2]), end=' ')
	dens = float(ans[1]) / (float(ans[0]) * (float(ans[0]) - 1) / 2)
	print("The graph density is %.5f." % dens)
	j = j.decode("utf-8").strip()
	ans = j.split()
	print("Overall %s men, %s women, and %s eunuchs" % (ans[0], ans[1], ans[2]))

# used only in development.
def testingGraphs(who, start, end, interval, minsize=5, preint=0, postint=20, \
		vag=False, sing=False): # runs tests on the gender graphs to determine cutoff
	for i in range(interval):
		for j in range(interval):
			genderGraph(who, start + i, end + j, vague=False, pre=0, post=20, singles=False)
			temp = subprocess.Popen('ccomps -x rel_graph.dot | gvpr -c \
				"N[nNodes($G)<%s]{delete(0,$)}" | dot | gvpack | sfdp -Goverlap=prism \
				| gvmap -e | gc -a' % str(minsize), shell=True, stdout=subprocess.PIPE, \
			stderr=subprocess.STDOUT)
			for line in temp.stdout.readlines():
				oput = line
			oput = oput.decode("utf-8")
			ans = oput[:len(oput)-17].strip().split() #-17
			print("Covering %s to %s, with %s nodes, %s edges, and %s connected components." \
				% (start + i, end + j, ans[0], ans[1], ans[2]), end =" ")
			dens = int(ans[1]) / (int(ans[0]) * int(ans[0]) - 1 / 2)
			print("Density: %s." % dens)

# reads a period[E\M\L] and century converts date to four digit format (!!)
def dateEqui(period, century):
	temp = int(century)* 10
	dating = ['E', 'M', 'L']
	if period == 'a': temp += 5
	else: temp += dating.index(period) * 3 + 3
	return temp

# returns number and descriptive statistics of people within vague limits
def descGenderVague(who, st, cent1, en, cent2):	
	men = women = eunuchs = others = 0
	earlylim = dateEqui(st, cent1)
	latelim = dateEqui(en, cent2)
	n = 0

	for i, val in who.items():
		#print(val.name, val.floruit)
		starttar = dateEqui(val.floruit[0], val.floruit[1])
		endtar = dateEqui(val.floruit[2], val.floruit[3])
		belongs = False
		if earlylim > endtar or latelim < starttar or endtar > latelim: belongs = False
		elif starttar >= earlylim and starttar <= latelim or \
			endtar >= earlylim and endtar <= latelim: belongs = True
		if belongs == True:
			if val.gender == 'Male': men += 1
			elif val.gender == 'Female': women += 1
			elif val.gender == 'Eunuch': eunuchs += 1
			else: others += 1
		n = men + women + eunuchs + others
		
	print("Vague dates, %s %s-%s %s: %s overall, %s men, %s women, %s eunuchs, %s others." % \
		(st, cent1, en, cent2, n, men, women, eunuchs, others), end="")
	wm = women / n
	em = eunuchs / n
	wem = (women+eunuchs) / n
	print(" W/N: %.3f, Eu/N ratio: %.3f, W+E/N ratio: %.3f" 
		% (wm, em, wem))
	

# descriptive statistics on gender
def descGender(who, start, end): # returns number of each gender between inclusive years
	# note that this still includes everyone (i.e. inc. crusaders and foreigners)
	# covers only people with a concrete median date
	men = women = eunuchs = others = 0
	for i, val in who.items():
		if val.mdate >= start and val.mdate <= end:
			if val.gender == 'Male': men += 1
			elif val.gender == 'Female': women += 1
			elif val.gender == 'Eunuch': eunuchs += 1
			else: others += 1
	n = men + women + eunuchs + others
	print("Specific dates, %s-%s: %s overall, %s men, %s women, %s eunuchs, %s others." % \
		(start, end, n, men, women, eunuchs, others), end="")
	wm = women / n
	em = eunuchs / n
	wem = (women+eunuchs) / n
	print(" W/N ratio: %.3f, Eu/N ratio: %.3f, W+E/N ratio: %.3f" 
		% (wm, em, wem))

# tests on gender to determine cutoff points. Relevant only for development.
def testingDescGender(who, start, end, minsize=5): 
	for i in range(minsize):
		for j in range(minsize):
			descGender(who, start + i, end + j)

# returns the ID of someone based on his name and IDB code
def getPerID(everyone, name, code):
	for i, val in everyone.items():
		if val.name[0] == name and val.name[1] == code: return i
	print("Couldn't find the person " + name + " " + code)


	# this extracts a list of the people
	# in Radolibos (E12C): 350 people (1103) or 49 people (1100)
	#from Radolibos. Returns a list of all their IDs. Requires original file.

# returns all individuals from Radolibos. 
# I created the file by copy-pasting the text from the two PBW entries	
def getRadolibos(everyone, filename):  
	runtime = time.time()
	f = open(filename, 'r') 
	num_lines = sum(1 for line in open(filename))
	n = 0
	paroikoi = []
	newline = f.readline()
	for i in range(num_lines):
		if '2.' not in newline:
			ind = newline.index('(')+1
			if newline[ind:ind+1].isupper() == True: p = newline[ind:len(newline)-2]
			else: 		# exception for two individuals
				abb = newline[ind:]
				p = abb[abb.index('(')+1:len(abb)-2]
			li = p.split()
			k = getPerID(everyone, li[0], li[1])
			if k not in paroikoi: # new person
				paroikoi.append(k)
				#print(p) # can print for blacklist, also below
				n += 1
			everyone[k].findRelatives()
			if everyone[k].relatives:
				for j in everyone[k].relatives:
					if j[1] not in paroikoi:
						paroikoi.append(j[1])
						n += 1
						# can print for blacklist, also above
						#print(everyone[j[1]].name[0] + " " + everyone[j[1]].name[1])
		newline = f.readline()
	f.close()	
	print("Runtime is " + str(round(time.time()-runtime, 3)) + " seconds.")
	print("Extracted %s people from Radolibos," % n, end=" ")
	m, f = 0, 0
	for i in paroikoi:
		if everyone[i].gender == 'Male': m += 1
		elif everyone[i].gender == 'Female': f += 1
	print("overall %s men, %s women." % (m, f))
	#print(paroikoi)
	return paroikoi

# this returns a subset of 'every' based on three criteria, meant to be used
# for blacklisting certain groups of individuals
def getSet(every, foreigners=True, vagueDate=True, document=True):
	# true values mean these categories would be returned in the set 'blacklist'

	# these use sets for faster performance
	docu = {'Basileios 142', 'Anna 107', 'Barbara 102', 'Blasios 105', 'Ioannikios 104', 
		'Demetrios 119', 'Basileios 143', 'Maria 114', 'Petros 117', 'Ioannes 192', 
		'Anastasia 103', 'Gnebotos 101', 'Maure 101', 'Stephanos 118', 'Ioannes 194', 
		'Basileia 101', 'Dragna 101', 'Ioannilos 102', 'Kyriake 101', 'Nikolaos 127', 
		'Ioannes 196', 'Anna 108', 'Barbara 103', 'Demetria 101', 'Kale 103', 
		'Maria 115', 'Nikolaos 131', 'Christina 102', 'Dobrana 101', 'Helena 108', 
		'Nikolaos 128', 'Nikolaos 129', 'Zacharias 103', 'Nikephoros 127', 'Anna 105', 
		'Ioannikios 103', 'Theodoros 138', 'Nikolaos 130', 'Georgios 145', 
		'Kaloudes 101', 'Pantoleon 102', 'Dobrobetos 101', 'Paulos 106', 'Agathe 103', 
		'Kyriakos 107', 'Theodoros 139', 'Anastasia 104', 'Berchoblabos 101', 
		'Chiono 101', # all these are from Radolibos 1100. 49 people, 29 men, 20 women
		'Anna 113', 'Anonymi 144', 'Anonymus 341', 'Anonymus 333', 'Anna 109',
	 	'Anonymus 334', 'Anonymus 335', 'Anna 111', 'Anonymus 336', 'Basileios 146', 
	 	'Deadole 102', 'Anonymus 338', 'Eugeno 101', 'Nikolaos 138', 'Anonymus 339', 
	 	'Eirene 106', 'Theodoros 142', 'Anonymus 340', 'Anonyma 138', 'Basileios 147', 
	 	'Ioannes 205', 'Anonymus 342', 'Andreas 102', 'Anonyma 140', 'Georgios 152', 
	 	'Anonymus 343', 'Anonymus 344', 'Marina 101', 'Anonymus 345', 'Dobrana 104', 
	 	'Stankos 101', 'Anonymus 347', 'Basileios 154', 'Petros 120', 'Rosanna 102', 
	 	'Anonymus 348', 'Ioannes 210', 'Kale 106', 'Anonymus 350', 'Milo 101', 
	 	'Nikolaos 145', 'Anonymus 351', 'Basileios 157', 'Kalonas 102', 'Stogisa 101', 
	 	'Anonymus 352', 'Anonyma 145', 'Ioannes 216', 'Nikolaos 146', 'Pankalos 101', 
	 	'Anonymus 353', 'Belkonas 104', 'Zoe 105', 'Anonymus 357', 'Maria 120', 
	 	'Nikephoros 130', 'Anonymus 358', 'Ioannes 224', 'Maria 121', 'Mikre 101', 
	 	'Basileios 145', 'Nikolaos 134', 'Zakchaios 101', 'Basileios 148', 
	 	'Basileios 149', 'Anonyma 141', 'Paulos 109', 'Stanka 101', 'Basileios 151', 
	 	'Anonymus 359', 'Demetrios 123', 'Demetrios 142', 'Epiphanios 101', 
	 	'Kalanna 102', 'Konstantineba 101', 'Konstas 101', 'Basileios 155', 
	 	'Kristilas 101', 'Maritza 104', 'Belkonas 102', 'Kale 108', 'Paulos 110', 
	 	'Stephanos 124', 'Stephanos 125', 'Belkonas 103', 'Anna 115', 'Kyrillos 102', 
	 	'Maritza 105', 'Choudinas 101', 'Anonyma 142', 'Kalkos 102', 
	 	'Christophoros 105', 'Anonyma 133', 'Georgios 149', 'Chrysonas 102', 
	 	'Georgios 154', 'Kalkos 103', 'Maria 116', 'Demetrios 121', 'Stephanos 121', 
	 	'Theodoros 144', 'Zoe 104', 'Demetrios 122', 'Konstantinos 149', 
	 	'Basileios 153', 'Kale 105', 'Therianos 101', 'Demetrios 127', 'Anonyma 155', 
	 	'Ioannes 221', 'Ioannes 222', 'Ioannitzes 101', 'Malka 101', 'Petros 118', 
	 	'Dobetzeros 101', 'Anonyma 137', 'Dobranos 101', 'Anonymus 349', 
	 	'Demetrios 124', 'Kale 107', 'Dobrases 101', 'Hemeroba 101', 'Simerinos 101', 
	 	'Theophano 101', 'Dobrotas 103', 'Marouda 101', 'Nikolaos 135', 'Slinas 101', 
	 	'Dobrotas 105', 'Chryse 103', 'Nikolaos 140', 'Dobrotas 106', 'Mera 102', 
	 	'Stephanitzes 101', 'Theodoros 145', 'Dobrousa 101', 'Ioannes 219', 
	 	'Nikolaos 149', 'Photeine 103', 'Dokeianos 101', 'Anonyma 144', 
	 	'Pachypodes 101', 'Eustathios 110', 'Koukla 101', 'Eustathios 112', 
	 	'Kougeres 101', 'Kougeroba 101', 'Sneagola 101', 'Georgios 151', 'Asana 101', 
	 	'Eustathios 109', 'Theodoros 146', 'Georgios 156', 'Anna 114', 'Komes 101', 
	 	'Georgios 158', 'Stephanos 126', 'Theophano 102', 'Georgios 161', 
	 	'Basileios 158', 'Eirene 107', 'Gerkos 101', 'Draga 102', 'Ioannes 212', 
	 	'Nikolaos 144', 'Ioannes 201', 'Anonyma 135', 'Dobrotas 102', 'Ioannes 202', 
	 	'Anonymus 337', 'Maritza 103', 'Ioannes 206', 'Chrysonas 101', 'Ioannes 208', 
	 	'Mikronas 101', 'Ioannes 209', 'Basileios 152', 'Deadole 102', 'Ioannes 214', 
	 	'Helena 109', 'Ioannes 215', 'Sarantenos 101', 'Sthlabitza 102', 'Ioannes 217', 
	 	'Maria 117', 'Nikolaos 147', 'Ioannes 218', 'Eirene 108', 'Helena 110', 
	 	'Konstantinos 157', 'Prodanos 101', 'Ioanni... 101', 'Anonyma 134', 
	 	'Basileios 144', 'Kalkos 104', 'Anna 118', 'Basileios 159', 'Anonyma 139', 
	 	'Konstantinos 152', 'Margarito 101', 'Niketas 128', 'Theodoros 151', 
	 	'Konstantinos 153', 'Krino 101', 'Konstas 102', 'Anonymus 354', 'Neanga 102', 
	 	'Konstas 103', 'Ioannes 220', 'Kale 111', 'Kra[..]rouses 101', 'Georgios 153', 
	 	'Tzernanga 101', 'Kyriake 102', 'Anonymus 355', 'Leon 132', 'Anna 117', 
	 	'Konstantinos 155', 'Themeles 101', 'Malleses 101', 'Anna 110', 'Michael 166', 
	 	'Chrysos 102', 'Kalanna 101', 'Michael 168', 'Drazitza 101', 'Georgios 155', 
	 	'Michael 169', 'Ioannes 213', 'Kale 109', 'Paraskeue 101', 'Michael 170', 
	 	'Konstantinos 158', 'Maria 118', 'Paschales 102', 'Michael 171', 'Konstas 104', 
	 	'Nedanitzes 101', 'Photeine 102', 'Tzaikos 101', 'Nedanos 102', 'Kyprianos 101', 
	 	'Teichole 101', 'Nikephoros 128', 'Anna 120', 'Kale 112', 'Nikephoros 129', 
	 	'Nikolaos 133', 'Photeine 101', 'Nikolaos 136', 'Anonymi 143', 'Charitza 101', 
	 	'Demetrios 144', 'Kale 104', 'Nikolaos 137', 'Anonyma 136', 'Paulos 107', 
	 	'Chryse 102', 'Petros 119', 'Nikolaos 142', 'Basileios 150', 'Dobrana 103', 
	 	'Nixas 101', 'Theodoros 147', 'Nikolaos 143', 'Georgia 101', 'Ioannes 211', 
	 	'Stanka 102', 'Nikolaos 148', 'Konstantinos 156', 'Teknodote 101', 'Paulos 108', 
	 	'Michael 167', 'Neanga 101', 'Paulos 111', 'Demetrios 125', 'Demetrios 126', 
	 	'Krino 102', 'Paulos 112', 'Anonymus 356', 'Basileios 160', 'Maria 119', 
	 	'Theodoros 154', 'Draga 101', 'Georgios 150', 'Ioannes 203', 'Ioannes 204', 
	 	'Petros 121', 'Kosanna 101', 'Petros 122', 'Anna 116', 'Georgios 160', 
	 	'Romanos 107', 'Basileios 156', 'Gabrilas 101', 'Gabriloba 101', 'Georgios 159', 
	 	'Kyranna 101', 'Nikolaos 173', 'Rosanna 103', 'Alexios 101', 'Konstantinos 154', 
	 	'Theodoros 148', 'Stephanos 120', 'Anonyma 132', 'Ioannes 207', 'Mera 101', 
	 	'Stephanos 122', 'Anonymus 346', 'Dra... 101', 'Stephanos 123', 'Rosanna 101', 
	 	'Symeon 112', 'Anonyma 143', 'Georgios 157', 'Stephanos 127', 'Basilo 101', 
	 	'Demetrios 128', 'Ioannes 223', 'Sthlabotas 102', 'Dobrotas 104', 
	 	'Tzinagoules 101', 'Sthlabotas 103', 'Dobrana 102', 'Dragotas 101', 
	 	'Kalkos 101', 'Chryse 104', 'Symeon 113', 'Georgios 162', 'Kale 110', 
	 	'Theodoros 143', 'Chryse 101', 'Chrysos 101', 'Mauros 102', 'Nikolaos 139', 
	 	'Eustathios 111', 'Mera 103', 'Nikolaos 141', 'Tzernes 102', 'Theodoros 149', 
	 	'Sthlabitza 101', 'Theodoros 150', 'Theodoros 152', 'Anonyma 146', 
	 	'Anonymus 385', 'Anna 112', 'Theodoros 141', 'Zoe 106', 'Nikolaos 150', 
	 	'Theodoros 153'} # these are from Radolibos 1103, 350 ppl, 239 men, 111 women
	foreig = {'Gagik 101', 'David 108', 'Tughrul Beg 51', 'Asan 101', 
		'Ibrahim Yinal 101', 'Malik-Shah 51', 'Kutulmush 101', 'Abimelech 101',
		'Amer 101', 'Amer 102', 'Liparites 101', 'Thimal 101', 'Nasr 101',
		'Salih 101', 'Muqallad 101', 'Tyrach 101', 'Kegen 101', 'Baltzar 102',
		'Robert 61', 'Drogo 10101', 'Humphrey 10101', 'Bagrat 101', 'Hakim 101',
		'Al-Zahir 101', 'Apochaps 101', 'Apolaphar Mouchoumet 101', 'Sinan 101',
		'Pinzarach 101', 'Ashot 101', 'Aplesphares 101', 'Phatloum 101', 'Vahram 101',
		'Mousaraph 101', 'Alach 101', 'Alde 101', 'Aleim 101', 'Anonyma 114',
		'Anonyma 123', 'Anonymae 102', 'Anonymus 138', 'Anonymus 160', 'Anonymus 161',
		'Anonymus 194', 'Artaseiras 101', 'Aspan 101', 'Georgios 105', 'Goulinos 101',
		'Ioannes 106', 'Jaroslav 101', 'Mahmud 102', 'Melech 101', 'Mousaraph 101',
		'Nesisthlabos 101', 'Paulos 125', 'Petros 102', 'Salamanes 101', 'Voislav 101',
		'Rafi 101', 'Auberee 101', 'Anonyma 210', 'Petronius 10101', 'William 10101',
		'Anonymus 15037', 'Anonyma 156', 'Anonymus 402', 'Atom 101', 'Ivane 101', 
		'Gregorios 127', 'Petros 128', 'Frederick 101', 'Smaragdos 101', 'Melech 101',
		'Xachik 101', 'Ananias 101', 'Leon 29', 'Maria 103', 'Georgios 105',
		'Senachereim 101', #guy who joined 1022	# up to here covers 1025-1059
		'Alp Arslan 51', 'Anonyma 103', 'Anonyma 106', 'Anonymi 106', 'Anonymi 110',
		'Malapetzes 280', 'Anonymus 319', 'Gaita 101', 'Helena 101', 'Konstantinos 102',
		'Michael 101', 'Mansur 5000', 'Anonymae 2102', 'William 10102', 'Belfeth 4001',
		'Anonyma 15008', 'Anonyma 15009', 'Robert 102', 'Euboulos 15001', 'Raoul 15001',
		'Raymond 15001', 'Amadeus 101', 'Beatrice 101', 'Anonymus 939', 'Anonymus 940',
		'Anonyma 102', 
		# up to here covers 1059-1081
		'Constance 4001', 'Lethold 4001', 'Anonymi 11001', 'William 4008', 'Anonyma 4003',
		'Anonymus 15086', 'Renaud 4002', 'Gazes 101', 'Allan 101', 'William Jordan 4001',
		'Anonymus 15066', 'Anonyma 4006', 'Joscelin 4001', 'Shams al-Dawla 26101', 
		'Matilda 101', 'Anonymus 873', 'Aubrey 4002', 'Dukak 4001', 'Ida 26102',
		'Simon 102', 'Anonyma 4009', 'Otto 26101', 'Hugh 4002', 'Pisellus 26101',
		'Tzachas 61', 'Ruthard 4001', 'Guy 26101', 'Tutush 101', 'Zanki 101', 
		'Anonymus 15084', 'Garnier 4001', 'Taftoc 4001', 'Anonyma 214', 'Gozelo 4001',
		'Reinhard 4001', 'Anonyma 243', 'Arnulf 4001', 'Bursuq 4001', 'Bertrand 4001', 
		'Sulayman 5000', 'Hugh 4008', 'Baldwin 4004', 'Radulf 107', 'Melikshah 101', 
		'Richard 4001', 'Eustace 4001', 'Raymond 61', 'Walter 4002', 'Henry 4001',
		'Anonymus 15106', 'William 4014', 'Anonyma 26104', 'Robert 26103', 'Godfrey 51',
		'Sultan 101', 'Guy 4004', 'Philippos 4001', 'Murshid 101', 'Sven 4001',
		'Anonymus 849', 'Cono 4001', 'Hugh 4010', 'Anonymus 26166', 'Firuz 4001', 
		'Hugh 4001', 'Baldwin 52', 'Engelrand 4001', 'Anonymus 862', 'Anonymi 4001', 
		'Faris 101', 'Albert 26102', 'Ridwan 4001', 'Morphia 4001', 'Ilghazi 4001', 
		'Buldagis 26101', 'Emelota 4001', 'Atenellus 26101', 'Robert 4009',
		'Ouresis 15001', 'Anonymi 26103', 'Godfrey 4003', 'Muhammad 26101', 
		'Anonymus 883', 'Anonyma 4007', 'Hugh 102', 'Anonymi 200', 'Poulchases 15001',
		'Conrad 26102', 'Kilic Arslan 51', 'Chokurmish 26101', 'Robert 63', 
		'Bolkanos 15001', 'Godehilde 4001', 'Magnus 26101', 'Anonymus 4022', 
		'Aksungur 4001', 'Louis 4001', 'Ishmael 15001', 'Anonymus 26135', 'Godfrey 26102',
		'Sokman 26101', 'Roger 15001', 'Anonymus 4032', 'Sultan-Shah 101', 
		'Kerbogha 4001', 'Tancred 61', 'Anonymus 856', 'Bernard 26101', 'Lambert 4002',
		'Bohemond 61', 'Anonyma 4005', 'Henry 55', 'Anonymus 4023', 'William 26103', 
		'Eustace 4003', 'Tughtigin 4001', 'Franco 4001', 'Roger 4002', 'Joseph 115', 
		'Anonymus 26158', 'Godfrey 4004', 'Cecilia 102', 'Baldwin 51', 'Adelaide 4001', 
		'Henry 54', 'Sigemar 4001', 'Ivo 4001', 'Engilbert 4001', 'Cecilia 4001', 
		'Anonymus 26124', 'Yaghi Siyan 4001', 'Richard 103', 'Stephanos 4003', 
		'Anonymus 26125', 'Bohemond 26101', 'Stephanos 15002', 'Gerard 26107', 
		'Abul-Kasim 15001', 'Adelbero 26101', 'William 4007', 'Mabel 4001',
		'Stephanos 4001', 'Barkiyaruq 101', 'Walter 26101', 'Renaud 26102', 
		'Stephanos 4002', 'Roger 4003'	# these are foreigners 1081-1120
		'Toros 101', 'Konstantinos 4002', 'Siaous 15001', 'Kogh Vasil 4001', 
		'Pankratios 4001', 'Roger 15002', 'Katakalon 61', 'Thoros 101', 'Gabriel 4001',
		'Anonyma 235', 'Raul 101', 'Anonyma 232', 'Oshin 20101'} #Armenian princes & allies

	vagueDate = set()

	# unions all sets based on the function's parameters
	blacklist = set()
	if foreigners == True: blacklist = blacklist.union(foreig)
	if document == True: blacklist = blacklist.union(docu)
	if vagueDate == True: blacklist = blacklist.union(vagueDate)
	
	if foreigners == True: print("Blacklist includes foreigners. ", end="")
	else: print("Blacklist does not include foreigners. ", end="")
	if document == True: print("Blacklist includes documentary evidence. ", end="")
	else: print("Blacklist does not include documentary evidence. ", end="")
	if vagueDate == True: print("Blacklist includes vague-date individuals. ", end="")
	else: print("Blacklist does not include vague-date individuals. ", end="")
	print()

	return blacklist

# removes all individuals in the blacklist from 'every'
def removeBList(every, blist):
	newDict = {}
	for i, val in every.items():
		name = "%s %s" % (val.name[0], val.name[1])
		if name not in blist: newDict[i] = val
		#else: print(name)		
	return newDict

# this is the main function to run for the kin group graph-creation.
# It results in four graphs (png files) and descriptive data printed to std output.
def genderTests(every, rel): 
	# calculate all people in this range
	descGenderVague(rel, 'E', 11, 'E', 12)
	# calculate people with a median date
	descGender(rel, 1025, 1118)
	
	# blacklist the foreigners and individuals that appear only on documents.
	blacklisted = getSet(rel, vagueDate=False)
	
	# return descriptive statistics after filtering out the blacklist, cutoff dates at 1055/6, 1080/1
	descGender(removeBList(rel, blacklisted), 1025, 1118)
	descGender(removeBList(rel, blacklisted), 1025, 1055)
	descGender(removeBList(rel, blacklisted), 1056, 1080)
	descGender(removeBList(rel, blacklisted), 1081, 1118)

	# print graphs for the large period and the three subperiods
	printGenGraph(every, 1025, 1118, vag=True, doc=True, preint=5) # takes several seconds
	printGenGraph(every, 1025, 1055, vag=False, doc=False, preint=5, postint=20) #True bool = yes display
	printGenGraph(every, 1056, 1080, vag=False, doc=False, preint=5, postint=20)
	printGenGraph(every, 1081, 1118, vag=False, doc=False, preint=5, postint=20)





###########################################################
############### eunuch part functions #####################
###########################################################

#draws the selected eunuch graph; only one of the parameters can be true.
# offices=True - keeps emperor nodes of same size, weights eunuchs based on their offices
# emperorweight=True - keeps eunuch nodes of same size, weights emperors based on the 
#				number of eunuchs who first appeared divided by the length of their reign
def eunuchGraph(offices=False, emperorweight=False):
	graph = pydot.Dot(graph_type='graph')
	# emperor = name: [start year, end year, eunuchs that first appear divided by regnal years]
	emperors = {'Basileios 2': [976, 1025, 0.18], 'Konstantinos 8': [1025, 1028, 2.33], 
		'Romanos 3': [1028, 1034, 0.16], 'Michael 4': [1034, 1041, 1], 
		'Michael 5': [1041, 1042, 2], 'Zoe 1': [1042, 1042, 0], 
		'Theodora 1': [1055, 1056, 0], 'Konstantinos 9': [1042, 1055, 0.84], 
		'Konstantinos 10': [1059, 1067, 0], 'Michael 6': [1056, 1057, 0],
		'Isaakios 1': [1057, 1059, 0], 'Michael 7': [1071, 1078, 0.28],
		'Nikephoros 3': [1078, 1081, 0.33], 'Alexios 1': [1081, 1118, 0.35], 
		'Eudokia 1': [1067, 1067, 0], 'Romanos 4': [1067, 1071, 0]}
	emperorConnections = {('Basileios 2', 'Konstantinos 8'), 
			('Konstantinos 8', 'Romanos 3'), ('Romanos 3', 'Michael 4'), 
			('Michael 4', 'Michael 5'), ('Michael 5', 'Zoe 1'),
			('Zoe 1', 'Konstantinos 9'), ('Konstantinos 9', 'Theodora 1'),
			('Theodora 1', 'Michael 6'), ('Michael 6', 'Isaakios 1'),
			('Isaakios 1', 'Konstantinos 10'), ('Konstantinos 10', 'Eudokia 1'),
			('Eudokia 1', 'Romanos 4'), ('Romanos 4', 'Michael 7'), 
			('Michael 7', 'Nikephoros 3'), ('Nikephoros 3', 'Alexios 1')}
	
	# add emperors to the graph and link them together
	for i in emperors:
		if offices == True: # scaling emperors if eunuch offices are weighted
			n = pydot.Node(i, fontcolor='purple', fontsize='40', height='1', width='1')
		elif emperorweight == True: # weighting emperors if requested
			empsize = 8 + 20 * emperors[i][2]
			n = pydot.Node(i, fontcolor='purple', fontsize=empsize)
		else: # emperors are standard in size
			n = pydot.Node(i, fontcolor='purple', fontsize='32', height='1', width='1')
		graph.add_node(n)
	for j in emperorConnections:
		adminlink = pydot.Edge(j[0], j[1], color='green', minlen=2.0, penwidth='4')
		graph.add_edge(adminlink)

	# this refers to positions that were not part of the imperial administration. 
	nondirect = ['Anonymus x27488C', 'Symeon x27488', 'Paulos x26352', 
				'Arsenios x20609', 'Philokales x26626', 'Ioannes x23370',
				'Stephanos 102', 'Leontakios 5000', 'Anonymus 5010',
				'Ioannikios 15001', 'Nikolaos 125', 'Michael 15011', 
				'Basileios 138', 'Basileios 140', 'Konstantinos 145', 
				'Gregorios 109', 'Ioannes x23370', 'Ioannes l3000',
				'Michael l3000'
				]

	eunuchlist = addEunuchDates('eunuchoff.txt')
	for i, val in eunuchlist.items():	# name: [[offices], highest rank, [years]]
		if val[2] != []:
			eustart = val[2][0]
			euend = val[2][1]
			if offices == True: 
				size = int(val[1])
				e = pydot.Node(i, fontsize=size*9)
				print(i, size*9)				
			else: e = pydot.Node(i, fontcolor='blue')
			graph.add_node(e)
			for j in emperors:
				if (emperors[j][0] >= eustart and emperors[j][0] <= euend) or \
					(emperors[j][1] >= eustart and emperors[j][1] <= euend) or \
					(eustart >= emperors[j][0] and eustart <= emperors[j][1]) or \
					(euend >= emperors[j][0] and euend <= emperors[j][1]):
					if i in nondirect:
						servant = pydot.Edge(e, j, color='red', style='dotted', len=0.3, weight=5, fontsize='8')
					else: 
						servant = pydot.Edge(e, j, color='red', len=0.3, weight=5, fontsize='8')
					graph.add_edge(servant)

		else: print("No dates listed for %s (prob. in eunuch list B)" % i)

	# output everything to graphs, based on user choice. 
	# I did not use the PDFs, and continued working on the dot files in Gephi.
	print(graph)
	if offices == True:
		graph.write_dot('eunuch_office_weighted.dot')
		graph.write_pdf('eunuch_office_weighted.pdf')
	elif emperorweight == True:
		graph.write_dot('eunuch_emperor_weighted.dot')
		graph.write_pdf('eunuch_emperor_weighted.pdf')
	else: 
		graph.write_dot('eunuch_admin.dot')
		graph.write_pdf('eunuch_admin.pdf')


def getEunuchs(every): #takes dictionary, returns a set of eunuch tuples
# this uses the Python database version to get the eunuchs, so individuals
# whose gender has been updated here will be in this set. 
	eunuchs = set()
	for i, val in every.items():
		if val.gender == 'Eunuch': 
			eunuchs.add((val.name[0], val.name[1]))
	return eunuchs

def importEunuchOffices(eunuchlist, filename, cursor): # takes a
# set of eunuchs and queries the SQL database to save their offices
# in filename. New eunuchs should be added to eunuchlist before running 
# this function
	n = 0
	off = 0
	f = open(filename, 'w')
	for i in eunuchlist:
		n += 1
		if i[1][0] == 'l' or i[1][0] == 'x': continue # new DB entries not in PBW
		# 'x' refers to the PmbW database; 'l' refers to individuals I've found & added
		# note that the df.stdName here isn't 100% identical in form to the online version
		cursor.execute("""SELECT p.personKey, p.name, p.mdbCode, df.stdName FROM person p 
					INNER JOIN factoidperson fp ON fp.personKey = p.personKey 
					INNER JOIN factoid f ON fp.factoidKey = f.factoidKey 
					INNER JOIN dignityfactoid df ON df.factoidKey = f.factoidKey 
					WHERE p.name='%s' and p.mdbCode='%s'
					AND factoidTypeKey=6;""" % (i[0], i[1]))
		for row in cursor:
			temp = [str(x) for x in row]
			if len(temp[3]) > 0:	# some offices in the DB are empty
				temp.append(officeRank(temp[3]))
				print(temp)
				f.write("\t".join(temp) + "\n")
				off += 1
	print("Overall found %s eunuchs and %s offices. Saved to %s" % (n, off, filename))
	f.close()

def addEunuchDates(filename): # returns a list of eunuch offices+dates
# loads the eunuchs' dates, based on my research
# these dates are IMPRECISE and meant to anchor the eunuchs in imperial
# reigns. The format for dates is [start, end, 0 if clear/1 if vague]. Eunuchs are
# listed in the order they appear in my Google Docs list
	eun = {'Anonymus x27488C': [972, 975, 1], 'Petros x26496': [975,977,0],
		'Basileios x20925': [950, 985, 0], 'Anthes x20452': [975, 977, 0],
		'Leon x24532': [975, 980, 0], 'Symeon x27488': [975, 1000, 0],
		'Paulos x26352': [975, 1000, 0], 'Knttis x23591': [975, 980, 0], 
		'Arension x20609': [980, 1000, 0], 'Barnakumeon(?) x20818': [990, 1000, 0],
		'Ioannes x23163': [1000, 1010, 0], 'Philokales x26626': [1000, 1010, 0],
		'Romanos x26847': [990, 1000, 0], 'Sergios x27045': [1000, 1010, 0],
		'Eustathios x21876': [1020, 1024, 0], 'Orestes 101': [1024, 1030, 0],
		'Ioannes 68': [1020, 1041, 0], 'Ioannes x23370': [1020, 1040, 1],
		'Niketas 101': [1026, 1027, 0], 'Eustratios 102': [1026, 1027, 0],
		'Symeon 101': [1026, 1040, 0], 'Anonymus 143': [1026, 1040, 0], 
		'Michael 108': [1026, 1040, 0], 'Nikephoros 104': [1026, 1040, 0],
		'Nikolaos 101': [1026, 1045, 0], 'Niketas 102': [1029, 1033, 0], 
		'Anonymus 165': [1035, 1038, 0], 'Georgios 106': [1035, 1038, 0], 
		'Antonios 101': [1035, 1038, 0], 'Basileios 106': [1035, 1038, 0], 
		'Georgios 107': [1035, 1038, 0], 'Konstantinos 106': [1035, 1038, 0],
		'Konstantinos 64': [1035, 1041, 0], 'Konstantinos 13': [1042, 1060, 0],
		'Stephanos 102': [1043, 1050, 0], 'Ioannes 114': [1043, 1050, 0],
		'Stephanos 144': [1043, 1050, 1], 'Nikephoros 108': [1043, 1050, 0],
		'Basileios 109': [1043, 1050, 0], 'Konstantinos 115': [1043, 1050, 0],
		'Konstantinos 5002': [1043, 1050, 0], 'Ioannes 115': [1043, 1050, 0], 
		'Basileios 111': [1050, 1056, 0], 'Konstantinos 119': [1050, 1056, 0],
		'Niketas 107': [1050, 1056, 0], 'Manuel 103': [1050, 1056, 0],
		'Theodoros 105': [1050, 1056, 0], 'Nikephoros 63': [1050, 1077, 0],
		'Leontakios 5000': [1072, 1074, 0], 'Anonymus 5010': [1072, 1074, 0], 
		'Ioannes 102': [1072, 1090, 0], 'Ioannikios 15001': [1072, 1078, 0], 
		'Symeon 130': [1072, 1090, 0], 'Ioannes 64': [1079, 1080, 0],
		'Leon 15004': [1079, 1080, 0], 'Sabas l3000': [1079, 1080, 0],
		'Ioannes l3000': [1090, 1100, 0], 'Leon 15008': [1090, 1100, 0],
		'Nikolaos 125': [1090, 1100, 0], 'Michael 15011': [1090, 1100, 0], 
		'Michael l3000': [1090, 1100, 0], 'Basileios 138': [1090, 1100, 0], 
		'Basileios 140': [1090, 1100, 0], 'Basileios 252': [1090, 1100, 0], 
		'Konstantinos 304': [1090, 1100, 0], 'Konstantinos 145': [1090, 1100, 0],
		'Demetrios 103': [1090, 1100, 0], 'Eustathios 15001': [1090, 1100, 0],
		'Eustratios 11': [1090, 1100, 0], 'Basileios 251': [1090, 1100, 0], 
		'Theodoulos l3000': [1090, 1100, 0], 'Anonymus l3000': [1090, 1100, 0],
		'Ioannes 449': [1110, 1120, 1], 'Gregorios 109': [1090, 1100, 0],
		# eunuch list B starts here (not included in graph, this is a partial list):
		#'Ioannes 268': [1080, 1090, 0], 'Anonymus 146': [1030, 1032, 0], 
		#'Anonymus 7001': [1030, 1030, 0], 'Anonymus 173': [1039, 1040, 0],
		#'Anonymus 398': [1045, 1047, 0], 'Anonymus 7006': [1050, 1051, 0],
		#'Anonymus 105': [1067, 1067, 0], 'Anonymus 5008': [1074, 1076, 0],
		#'Basileios 204': [1085, 1090, 0], 'Anonymus 727': [1090, 1100, 0],
		#'Anonymus 888': [1090, 1100, 0], 'Anonymus 977': [1090, 1100, 0],
		#'Anonymus 980': [1090, 1100, 0]
		}

	inp = open(filename, 'r')
	num_lines = sum(1 for line in open(filename))
	seen = set()
	eunuchlist = {}
	
	# turn the file into a dictionary. Name: list of offices
	off = inp.readline().strip().split('\t')
	while off != [""]:
		name = off[1] + " " + off[2]
		if name not in seen: 
			seen.add(name)
			eunuchlist[name] = [off[3]]
		else: 
			temp = eunuchlist[name]
			temp.append(off[3])
			eunuchlist[name] = temp
		off = inp.readline().strip().split('\t')

	# merge the list of offices with the dates
	result = {}
	for i in eunuchlist:
		offices = eunuchlist[i]
		temp = 0
		for j in offices:	# get the highest office rank
			if int(officeRank(j)) > int(temp): temp = officeRank(j)
		if i in eun: dates = eun[i]
		else: dates = []	# fill in dates, if extant
		result[i] = [offices, str(temp), dates]

	
	# adds eunuchs for which there are no offices
	for i in eun:
		if i not in result:
			result[i] = [[], '1', eun[i]] # giving rank of 1 as default

	# factors in sources' descriptions:
	corrections = {'Ioannes 68':'4'}
	for i in result:
		if i in corrections:
			result[i] = [result[i][0], corrections[i], result[i][2]]
			print(result[i])

	return result

# takes an office and returns its importance (4=high, 1=low)
def officeRank(offic): 
	# ranks are organized: church \ army \ palace \ ranks
	off = offic.split()[0]
	off = off.lower()
	if off == 'epi' or off == 'megas' or off == 'man':
		off = " ".join(offic.split()[0:2])
		off = off.lower()
	highestRank = {'patriarch', 'ecumenical',\
					'domestikos', 'megas domestikos', \
					'nobelissimos', 'nobelissimus', 'paradynasteuon', 'hypersebastos', \
					'mesazon', 'orphanotrophos'
		}
	secondRank = {'metropolitan', 'archbishop', \
					'doux', 'katepan', 'droungarios', 'hetaireiarches', \
					'stratopedarches', 'katepano', 'megas hetaireiarches', 
					'megas droungarios', 'heraireiarches', \
					'protoproedros', 'proedros', 'logothetes', 'parakoimomenos', \
					'magistros', 'man of the emperor', 'man of', 'hypertimos',
					'protosynkellos'
		}
	thirdRank = {'bishop', 'abbot', 'hegoumenos', 'strategos', 'ethnarches', \
					'sebastophoros', 'koitonites', 'praipositos', 'protovestiarios', \
					'patrikios', 'vestarches', 'epi tou', 'epi tou kanikleiou', 
					'raiktor', 'chartophylax', 'synkellos'
		}
	lowestRank = {'priest', 'monk', 'presbyter', 'deacon', \
					'protospatharios', 'protonotarios', 'notarios', 'primikerios', \
					'vestes', 'vestiarios', 'anagrapheus', 'ostiarios', 'krites', \
					'judge', 'praitor', 'praetor', 'mystikos', 'rhaiktor', \
					'synkletikos'
		}

	if off in highestRank: return '4'
	if off in secondRank: return '3'
	if off in thirdRank: return '2'
	if off in lowestRank: return '1'
	print("Rank not in lists: %s." % off)




###########################################################
############### foreigner part functions ##################
########## currently not planned for development###########
###########################################################

#this function takes a dictionary and returns a set of all foreigners listed as such. TESTED.
def getForeigners(every):
	foreigners = set()
	for i, val in every.items():
		if val.ethnicity != []: 
			foreigners.add((val.name[0], val.name[1]))
	return foreigners

# this function takes an office and returns its rank (on 1-5[high] scale). 
# 0 means this is a foreign title not bestowed by Constantinople. 
def foreignOfficeRank(offic):
	off = offic.split()[0]
	off = off.lower()
	if off == 'patriarch of antioch' or off == 'megas' or off == 'man':
		off = " ".join(offic.split()[0:2])
		off = off.lower()
	imperialRank = {'augousta', 'basilis', 'despoina', 'augusta', 'empress', \
					'autokratorissa', 'basilissa'
					}
	highestRank = {'domestikos', 'megas domestikos', 'nobelissimos', \
					'sebastos', 'sebaste', 'kouropalates', 'protokouropalates', \
					'patriarch of antioch', 'protonobelissimos'\
					}
	secondRank = {'doux', 'katepan', 'hetaireiarches', 'megas hetaireiarches', \
					'stratopedarches', 'katepano', 'megas droungarios', \
					'protoproedros', 'proedros', 'protoproedrissa', 'logothetes', \
					'parakoimomenos', 'magistros', 'man of the emperor', 'man of', \
					'hypertimos', 'panhyperlampros', 'heraireiarches', \
					'megas primikerios'\
					}
	thirdRank = {'bishop', 'abbot', 'hegoumenos', 'synkellos', 'strategos', 'general', \
				'ethnarches', 'ethnarch', 'sebastophoros', 'patrikios', 'patrician', \
				'vestarches', 'stolarches', 'praipositos', 'anthypatos', 'hypatos', \
				'protovestes'
					}
	lowestRank = {'protospatharios', 'protonotarios', 'vestes', 'vestiarios', \
				'topoteretes', 'stratelates', 'episkeptites', 'vestiarites'\
					}

	foreignRanks = {'basileus', 'king', 'sultan', 'count', 'marquis', 'queen', \
				'emir', 'lord', 'vizier', 'tsar', 'prince', 'duke', 'pope', 'doge', \
				'emperor', 'autokrator', 'ruler', 'governor', 'seljuk', 'caliph', \
				'archon', 'tzar', 'bulgaria,', 'pinkernes', 'archbishop', 'master', 
				'patriarch', 'atabeg', 'selarios', \
				'meizoteros' #one taxpayer has this title-probably v.local
					}

	if off in imperialRank: return '5'
	if off in highestRank: return '4'
	if off in secondRank: return '3'
	if off in thirdRank: return '2'
	if off in lowestRank: return '1'
	if off in foreignRanks: return '0'
	print("Rank not in lists: %s." % off)

def importForeignerOffices(fSet, filename, cursor):
	n = 0
	off = 0
	f = open(filename, 'w')
	for i in fSet:
		n += 1
		if i[1][0] == 'l' or i[1][0] == 'x': continue # new DB entries not in PBW
		# note that the df.stdName here isn't 100% identical in form to the online version
		cursor.execute("""SELECT p.personKey, p.name, p.mdbCode, df.stdName FROM person p 
					INNER JOIN factoidperson fp ON fp.personKey = p.personKey 
					INNER JOIN factoid f ON fp.factoidKey = f.factoidKey 
					INNER JOIN dignityfactoid df ON df.factoidKey = f.factoidKey 
					WHERE p.name='%s' and p.mdbCode='%s'
					AND factoidTypeKey=6;""" % (i[0], i[1]))
		for row in cursor:
			temp = [str(x) for x in row]
			if temp[3] != '':	# some offices in the DB are empty
				if foreignOfficeRank(temp[3]): temp.append(foreignOfficeRank(temp[3]))
				else: 
					print(temp)
					temp.append(temp[3])
				f.write("\t".join(temp) + "\n")
				off += 1
	print("Overall found %s foreigners and %s offices. Saved to %s" % (n, off, filename))
	f.close()




###########################################################
######## factoid-based network part functions #############
###########################################################

# returns all the NarrativeUnitIDs for a given year. Works.
def getAnnualFactoids(cursor, year):
	factlist = []
	cursor.execute("""SELECT narrativeUnitID FROM narrativeunit 
		WHERE LEFT(fmkey, 4) = %s;""" % year)
	for row in cursor:
		temp = [str(x) for x in row]
		factlist.append(temp[0])
	return factlist

# returns a list of edges for every NarrativeUnitID. Works.
def getFactoidConnections(cursor, nuid):
	people = []
	edgelist = []
	cursor.execute("""SELECT nu.narrativeunitid, nu.dates, 
					fp.personKey, p.name, p.mdbCode FROM narrativeunit nu
				INNER JOIN narrativefactoid nf ON nu.narrativeUnitID = nf.narrativeUnitID
    			INNER JOIN factoidperson fp ON nf.factoidKey = fp.factoidKey
    			INNER JOIN person p ON fp.personKey = p.personKey
    			WHERE nu.narrativeUnitID = '%s' AND fp.fpTypeKey = 2 GROUP BY fp.personKey;
		""" % nuid)
	for row in cursor:
		temp = [str(x) for x in row]
		people.append(" ".join(temp[3:5]))
	for i in range(len(people)):
		j = i + 1
		while j < len(people):
			if people[i] > people[j]: tie = (people[i], people[j])
			else: tie = (people[j], people[i])
			edgelist.append(tie)
			j += 1
	return edgelist

# takes dates and returns all connections in them.
# instant runtime for at least 20 year intervals
def allConnections(start, end, cursor):
	conn = []
	sumL = 0
	runtime = time.time()
	while start <= end:
		l = getAnnualFactoids(cursor, start)
		for i in l:
			conn += getFactoidConnections(cursor, i)
		start += 1
		sumL += len(l)

	conn = sorted(conn)
	edges = []
	edges.append(conn[0])
	for i in conn:
		if i != edges[len(edges)-1]: edges.append(i)

	
	print("Got connections in " + str(round(time.time()-runtime, 3)) + " seconds.", end=" ")
	print("Analyzed %s factoids, overall %s unique edges found." % 
		(str(sumL), str(len(edges))))
	return edges

# returns an EDGES file with all the unique edges for a time interval
# when importing to cytoscape: there are no headers; use TAB and space to limit
# otherwise it will mess up the spaces
def massiveGraph(start, end, cursor):
	graph = networkx.Graph()
	conns = allConnections(start, end, cursor)
	runtime = time.time()
	n = 0
	for i in conns:
		first = i[0] + "\t"
		second = i[1] + "\t"
		first = first.replace(" ","-")	# for cytoscape to read better
		second = second.replace(" ","-") # for cytoscape to read better
		graph.add_edge(first, second)
		n += 1
	
	print("Drew %s-edge graph in %s seconds." % (n, round(time.time()-runtime, 3)))
	filename = "massive_" + str(start) + "_" + str(end) + ".edges"
	networkx.write_edgelist(graph, filename)
	
# returns a file with: year-#factoids-#individuals-#total edges-#unique edges
def factoidStats(start, end, cursor):
	tot = str(start) + "-" + str(end)
	f = open('factoidStats.tsv', 'w')
	while start <= end:
		conn = []
		indi = set()
		l = getAnnualFactoids(cursor, start)
		for i in l:
			conn += getFactoidConnections(cursor, i)

			# calculate individuals
		for i in conn:
			indi.update({i[0]})
			indi.update({i[1]})
			
		conn = sorted(conn)
	
		# calculate unique edges
		uniqueedges = []
		if len(conn) > 0: uniqueedges.append(conn[0])
		for i in conn:
			if i != uniqueedges[len(uniqueedges)-1]: uniqueedges.append(i)
			print(i)
		# removing one or two empty factoids that are used to hold years
		if str(start)[3] == '1': l = len(l) - 2
		else: l = len(l) - 1
		
		temp = [str(start), str(l), str(len(indi)), str(len(conn)), 
												str(len(uniqueedges))]
		f.write("\t".join(temp) + "\n")
		start += 1
		
	print("Calculated factoid stats for %s interval." % tot)
	f.close()



#####################################################################
#####################################################################
##################### main part of program ##########################
#####################################################################
#####################################################################


conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
cur = conn.cursor()
#settingup('basiclist.txt', cur)

#Basic stuff required for all subsequent analyses:
everyone = memoryload('basiclist.txt')
ethnicities(everyone)	# loads all entries with their ethnicities
loadMedian(everyone)
relev = relevantPeople(everyone) # removes people not in L10-E12C

#####################################################################
########un-comment the subsection you want to run ###################
########### (remove all '###' from that section) ####################
#####################################################################
# all the written analysis is in my (Lee Mordechai's) dissertation,##
#Costly Diversity: Transformations, Networks and Minorities, 976-1118
#####################################################################


######################  Chapter 3 subsection 1: Elite Families
#####################################################################
# data taken from Kazhdan and Ronchey's L'aristocrazia byzantina
# analysis was done in Excel 
# visualization was done in Tableau

# empty since nothing was done in Python


######################  Chapter 3 subsection 2: Eunuchs & administrations
#####################################################################
# data from Prosopography of Byzantine World database and personal research
# analysis & cleaning here (Python)
# visualization through Gephi

###importEunuchOffices(getEunuchs(everyone), 'eunuchoff.txt', cur)
###addEunuchDates('eunuchoff.txt')

# running this three times for all three graphs. 
# only one parameter can be True at a time
###eunuchGraph(offices=True, emperorweight=False)
###eunuchGraph(offices=False, emperorweight=True)
###eunuchGraph(offices=False, emperorweight=False)


######################  Chapter 3 subsection 3: Large-scale visualization
#####################################################################
# data from Prosopography of Byzantine World database and personal research
# cleaning & analysis here (Python)
# analysis & visualization through Cytoscape

# tests:
#getAnnualFactoids(cur, 1051)
#getFactoidConnections(cur, '25415')
#allConnections(1051, 1071, cur)

# create the large network graphs
###massiveGraph(1025, 1057, cur)
###massiveGraph(1058, 1080, cur)
###massiveGraph(1081, 1118, cur)

# an extra test
#factoidStats(1098,1098, cur)



######################  Chapter 3 subsection 4: Kin Networks (gender-marked)
#####################################################################
# data from Prosopography of Byzantine World database and personal research
# cleaning & analysis here (Python)
# visualization through command prompt (graphviz)

genderTests(everyone, relev) 


# clean up
cur.close()
conn.close()







#####################################################################
########################## end of file ##############################
#####################################################################


#####################################################################
#################### former scraps and comments #####################
#####################################################################

#for i, val in everyone.items():
#	if val.gender == 'Eunuch' and val.mdate > 1: 
#		print(str(val.mdate) + " " + val.name[0] + " " + val.name[1] + " " + val.name[2])


#FORMER TESTS I'VE RUN:
#testingDescGender(everyone, 1023, 1055)
#testingDescGender(everyone, 1055, 1078)
#testingDescGender(everyone, 1078, 1115)
#testingGraphs(everyone, 1058, 1079, 5)

#genderGraph(everyone , 1025.5, 1056.5, vague=False, pre=0, post=20, singles=False)
#genderGraph(everyone ,'E', '11', 0)	# needs path to C:\Program Files (x86)\Graphviz2.38\bin
# standard path is to: C:\Users\veredlee\AppData\Local\Programs\Python\Python36-32

#printGenGraph(everyone, 1025.5, 1056.5, minsize=5)

########### Foreigner part, currently not planned for development
#importForeignerOffices(getForeigners(everyone), 'foreignoff.txt', cur)

# to do:
# - make relations keep its records in a separate file and use 
#   it to load things faster.
#	- all the entries for a person, inc. date and relationships
#	- figure out a scheme to load all the PmbZ entries into DB
#	- count all the female/eunuch/male nodes on the graphs. 

#graph commands:

# creates a graph of all subgraphs with 3 nodes or more.
#ccomps -x rel_graph.dot | gvpr -c "N[nNodes($G)<3]{delete(0,$)}" | dot | gvpack | sfdp -Goverlap=prism | gvmap -e | neato -n2 -Ecolor=#55555522 -Tpng > output.png
