class target{
	def name, dependsOn, dependants, columnLevel, rowLevel,group

	target(name, dependsOn, dependants, columnLevel, rowLevel,group){
		this.name = name
		this.dependsOn = dependsOn
		this.dependants = dependants
		this.columnLevel=columnLevel
		this.rowLevel=rowLevel
		this.group=group
	}
}

//inputs=[new target('b',['m'],[],2,'null','null'),
//		new target('i',['n'],[],1,'null','null'),
//		new target('a',[],[],0,'null','null'),
//		new target('z',['i'],[],1,'null','null'),
//		new target('d',['u'],[],2,'null','null'),
//		new target('f',['w'],[],2,'null','null'),
//		new target('u',['m'],[],1,'null','null'),
//		new target('e',['i'],[],2,'null','null'),
//		new target('n',['m'],[],1,'null','null'),
//		new target('g',['z'],[],2,'null','null'),
//		new target('m',['a'],[],1,'null','null'),
//		new target('w',['u'],[],1,'null','null'),
//		new target('c',['n'],[],2,'null','null')]
//inputs[2].dependants=[inputs[10]]
//inputs[10].dependants=inputs[6,0,8]
//inputs[6].dependants=inputs[11,4]
//inputs[8].dependants=inputs[12,1]
//inputs[3].dependants=[inputs[9]]
//inputs[11].dependants=[inputs[5]]
//inputs[1].dependants=inputs[7,3]

ins=[new target('a',[],[],1,'null','null'),
	new target('b',['a'],[],1,'null','null'),
	new target('c',['a'],[],1,'null','null'),
	new target('d',['a'],[],1,'null','null'),
	new target('ad',['d'],[],1,'null','null'),
	]
ins[0].dependants=ins[1,2,3]
ins[2].dependants=[ins[3]]
ins[3].dependants=[ins[4]]
def numberNodes(target,group){
	i=0
	def number
	number = {tar,gr->
		tar.level=i
		tar.group=gr
		i+=1
		for (d in tar.dependants.sort{it.name}){
			number(d,gr)
		}
	}
	number(target,group)
}

def rowLvls(columnLevel,depList){
	levelList=[]
	/* filter depList into 
	 * levelList, a list of targets with the inputed column Level
	 */
	for (i in depList){
		if (i.columnLevel == columnLevel){
			levelList+=i
		}
	}
	/* filter out any targets that are not in the column Level
	 * from each target's dependsOn list and dependants list
	 * if dependsOn==[] then add to nulldepOnList
	 */
	nulldepOnList=[]
	for (i in levelList){
		//change dependsOn really a list or targets not names
		i.dependsOn=i.dependsOn.findAll{it in levelList.collect{it.name}}
		i.dependants=i.dependants.findAll{it in levelList}
		if (i.dependsOn==[]){
			nulldepOnList+=i
		}
	}
	/* Put nulldepOnList in alphabetical order
	 */
	nulldepOnList.sort{it.name}
	/* Assign group and rowLevel to nulldepOnList
	 */
	for (i in nulldepOnList){
		i.group=nulldepOnList.indexOf(i)
		i.rowLevel=0
	}
	/* Assign transitive dependencies to each target
	 */
	for (i in nulldepOnList){
		numberNodes(i,i.group)
	}

	/* Find the number of groups in the level by finding the 
	 * maximum of it.group in levelList
	 */
	numOfGroups=levelList.collect{it.group}.max()+1

	/* List of lists, each list has all the targets with the same group level
	 */
	grouplist=[]

	for (int i=0; i < numOfGroups;i++){
		grouplist+=[depList.findAll{it.group==i}]
	}
	rowLevelList=[]
	/* Sort each list in group list by rowLevel and then by alpha
	 * then join list to rowLevelList
	 */
	for (i in grouplist){
		i.sort{x,y->
			if (x.rowLevel==y.rowLevel){
				x.name <=> y.name
			}else{
				x.rowLevel <=> y.rowLevel
			}
		}
		rowLevelList+=i
	}

	return rowLevelList
}


