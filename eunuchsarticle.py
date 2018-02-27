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

	moreeunuchs = [ # list B of eunuchs; @@@ this was originally a list from the dissertation
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
		#	- In the case of some anonymus groups, I cataloged as Mixed (based on desc.)
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
		# as of 12.1.17 = 16 issues (all empty records)


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


#####################
#####################
### analysis part ###
#####################
#####################

#draws the selected eunuch graph; only one of the parameters can be true.
# offices=True - keeps emperor nodes of same size, weights eunuchs based on their offices
# emperorweight=True - keeps eunuch nodes of same size, weights emperors based on the 
#				number of eunuchs who first appeared divided by the length of their reign
def eunuchGraph(offices=False, emperorweight=False):
	graph = pydot.Dot(graph_type='graph')
	# emperor = name: [start year, end year, eunuchs that first appear divided by regnal years]
	# @@@ I made changes here based on the slightly different analysis in the social mobility
	#		article - namely emphasizing the social mobility rather than the amenability to eunuchs
	emperors = {'Basileios 2': [976, 1025, 0.22], 'Konstantinos 8': [1025, 1028, 2.33], 
		'Romanos 3': [1028, 1034, 0.5], 'Michael 4': [1034, 1041, 1.14], 
		'Michael 5': [1041, 1042, 2], 'Zoe 1': [1042, 1042, 0], 
		'Theodora 1': [1055, 1056, 0], 'Konstantinos 9': [1042, 1055, 1.07], 
		'Konstantinos 10': [1059, 1066, 0], 'Michael 6': [1056, 1057, 0],
		'Isaakios 1': [1057, 1059, 0], 'Michael 7': [1071, 1078, 0.85],
		'Nikephoros 3': [1078, 1081, 0.33], 'Alexios 1': [1081, 1118, 0.48], 
		'Eudokia 1': [1067, 1067, 1], 'Romanos 4': [1068, 1071, 0]}
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
	# @@@ 
	eunuchlist = addEunuchDates('eunuchoff2.txt')
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
	# @@@ changed all file names to @@@
	if offices == True:
		graph.write_dot('eunuch_office_weighted2.dot')
		graph.write_pdf('eunuch_office_weighted2.pdf')
	elif emperorweight == True:
		graph.write_dot('eunuch_emperor_weighted2.dot')
		graph.write_pdf('eunuch_emperor_weighted2.pdf')
	else: 
		graph.write_dot('eunuch_admin2.dot')
		graph.write_pdf('eunuch_admin2.pdf')


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
# reigns. 
# @@@ The format for dates is [start, end, 0 if relevant/1 if not relevant]. 
# @@@ Eunuchs with 1 for relevance are not considered in the analyses for the article
# @@@ 	and do NOT have a '1' in the Google Docs
# @@@ Eunuchs are listed in the order they appear in my *new* Google Docs list
# @@@ made changes here to conform to the Google Docs tab 'Article basis'
	eun = {
		# @@@ removed these from consideration since they began before Basil II
		#'Anonymus x27488C': [972, 975, 1], 'Petros x26496': [975,977,0],
		#'Basileios x20925': [950, 985, 0], 'Anthes x20452': [975, 977, 0],
		#'Leon x24532': [975, 980, 0], 'Symeon x27488': [975, 1000, 0],
		#'Paulos x26352': [975, 1000, 0], 'Knttis x23591': [975, 980, 0], 
		# Basil II's reign:
		'Arsenios x20609': [980, 1000, 0], 'Barnakumeon(?) x20818': [990, 1000, 0],
		'Ioannes x23163': [1000, 1010, 0], 'Philokales x26626': [1000, 1010, 0],
		'Romanos x26847': [990, 1000, 0], 'Sergios x27045': [1000, 1010, 0],
		'Eustathios x21876': [1020, 1024, 0], 'Orestes 101': [1024, 1030, 0],
		'Ioannes 68': [1020, 1041, 0], #'Ioannes x23370': [1020, 1040, 1],
		#'Ioannes x23165': [990, 1000, 1], 
		'Anonymus x31996': [990, 1000, 1], 'Anonymus x31997': [990, 1000, 1],
		# Constantine VIII:
		'Niketas 101': [1026, 1027, 0], 'Eustratios 102': [1026, 1027, 0],
		'Symeon 101': [1026, 1040, 0], 'Anonymus 143': [1026, 1040, 0], 
		'Michael 108': [1026, 1040, 0], 'Nikephoros 104': [1026, 1040, 0],
		'Nikolaos 101': [1026, 1045, 0], 
		# Romanos III: 
		'Niketas 102': [1029, 1033, 0], 'Anonymus 146': [1029, 1033, 0],
		'Anonymus 7001': [1029, 1033, 0],
		# Michael IV:
		'Anonymus 173': [1039, 1040, 0], #'Anonyma 179': [1039, 1040, 1], #wrong entry
		'Anonymus 165': [1035, 1038, 0], 'Georgios 106': [1035, 1038, 0], 
		'Antonios 101': [1035, 1038, 0], 'Basileios 106': [1035, 1038, 0], 
		'Georgios 107': [1035, 1038, 0], 'Konstantinos 106': [1035, 1038, 0],
		'Konstantinos 64': [1035, 1041, 0], 
		# Michael V:
		'Konstantinos 13': [1042, 1060, 0],
		# Constantine IX:
			# accepting the following two as Constantine's
		'Stephanos 102': [1043, 1050, 0], 'Ioannes 114': [1043, 1050, 0],
		#'Stephanos 144': [1043, 1050, 1], 
		'Nikephoros 108': [1043, 1050, 0],
		'Basileios 109': [1043, 1050, 0], 'Konstantinos 115': [1043, 1050, 0],
		'Konstantinos 5002': [1043, 1050, 0], 'Ioannes 115': [1043, 1050, 0], 
			# the following two are listed as irrelevant since uncertain they were eunuchs
		#'Basileios 111': [1050, 1056, 1], 'Konstantinos 119': [1050, 1056, 1],
		'Niketas 107': [1050, 1056, 0], 'Manuel 103': [1050, 1056, 0],
		'Theodoros 105': [1050, 1056, 0], 'Nikephoros 63': [1050, 1077, 0],
		'Ioannes l3001': [1043, 1050, 0], 'Anonymus 398': [1043, 1050, 0],
		'Anonymus 7006': [1043, 1050, 0],
		# Eudokia:
		'Anonymus 105': [1067, 1067, 0],
		# Michael VII:
		'Leontakios 5000': [1072, 1074, 0], 'Anonymus 5010': [1072, 1074, 0],
		'Anonymus 5008': [1074, 1075, 0], 'Ioannes 102': [1072, 1090, 0], 
		'Ioannikios 15001': [1072, 1078, 0], 'Symeon 130': [1072, 1090, 0], 
		# Nikephoros III:
		'Ioannes 64': [1079, 1080, 0], 'Leon 15004': [1079, 1080, 0], 
		'Sabas l3000': [1079, 1080, 0],
		# Alexios I
		'Ioannes l3000': [1090, 1100, 0], 'Leon 15008': [1090, 1100, 0],
		'Nikolaos 125': [1090, 1100, 0], 'Michael 15011': [1090, 1100, 0], 
		'Michael l3000': [1090, 1100, 0], 'Basileios 138': [1090, 1100, 0], 
		'Basileios 140': [1090, 1100, 0], 'Basileios 252': [1090, 1100, 0], 
		'Konstantinos 304': [1090, 1100, 0], 'Konstantinos 145': [1090, 1100, 0],
		'Demetrios 103': [1090, 1100, 0], 'Eustathios 15001': [1090, 1100, 0],
		'Eustratios 11': [1090, 1100, 0], 'Basileios 251': [1090, 1100, 0], 
		'Theodoulos l3000': [1090, 1100, 0], 'Anonymus l3000': [1090, 1100, 0],
		'Gregorios 109': [1090, 1100, 0], 'Ioannes 268': [1084, 1085, 0],
		'Basileios 204': [1085, 1090, 0], 'Anonymus l3001': [1084, 1085, 0],
		#'Anonymus 980': [1083, 1085, 0], 
		'Anonymus 888': [1090, 1100, 0],
		#'Anonymus 977': [1075, 1100, 1], 
		'Anonymus 727': [1090, 1100, 0],
		#'Anonymus l3002': [1075, 1100, 1], 
		'Anonymus l3003': [1085, 1090, 0],
		'Anonymus l3004': [1075, 1100, 0], 'Leon 177': [1085, 1090, 0],
		#'Ioannes 449': [1110, 1120, 1] 
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


########################
########################
### main part to run ###
########################
########################


#######################
###### setting up #####
#######################

# code based on dissertation work (the file basics.py on GitHub)
# data from Prosopography of Byzantine World database and personal research
#	places with changes from the original analyses marked with @@@
# analysis & cleaning here (Python)
# visualization through Gephi

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='1234', db='pbw')
cur = conn.cursor()
#settingup('basiclist.txt', cur)

#Basic stuff required for all subsequent analyses:
everyone = memoryload('basiclist.txt')
ethnicities(everyone)	# loads all entries with their ethnicities
relev = relevantPeople(everyone) # removes people not in L10-E12C

######################################
###### can change the part below #####
######################################


# @@@ changed file name to eunuchoff2
importEunuchOffices(getEunuchs(everyone), 'eunuchoff2.txt', cur)
addEunuchDates('eunuchoff2.txt')

# running this three times for all three graphs. 
# only one parameter can be True at a time
# @@@ the easiest way to work all three graphs in Gephi is with offices=True
# @@@ and emperorweight=False; this way can do the hard work manually in Gephi
# @@@ (it's faster and more standard than creating 3 separate .dot files).

eunuchGraph(offices=True, emperorweight=False)
###eunuchGraph(offices=False, emperorweight=True)
###eunuchGraph(offices=False, emperorweight=False)

