from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("HeroePopular")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)    

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))   #se extrae el ID (clave,valor)

names = sc.textFile("file:///CursoSpark/marvel-names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("file:///CursoSpark/marvel-graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y) # total de apariciones
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))   # switch de valores

mostPopular = flipped.max()     # Se conoce el heroe mas popular

mostPopularName = namesRdd.lookup(mostPopular[1])[0]    # el [0] es porque al buscar, se obtiene la fila 1 del lookup

print(str(mostPopularName) + " es el heroe mas popular, con " + \
    str(mostPopular[0]) + " co-apariencias.")
