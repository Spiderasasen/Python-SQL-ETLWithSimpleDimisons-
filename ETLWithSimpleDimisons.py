import pymysql
import configparser
import csv

def readConfig(configFile):

    # Get credentials to connect to database
    config = configparser.ConfigParser()
    try:
        config.read_file(open(configFile)) # point to the correct place where this file is!
        dbConfig = {
            "host" : config['csc']['dbhost'],
            "user" : config['csc']['dbuser'],
            "password" : config['csc']['dbpw']
        }
        return dbConfig
    except FileNotFoundError as e:
        print(f"{configFile} was not found")
        raise

def connectToDatabase(dbName,configFile):

    #read in connection info
    dbConfig = readConfig(configFile)

    #Open database connection
    dbConn = pymysql.connect(host=dbConfig["host"],
                             user=dbConfig["user"],
                             passwd=dbConfig["password"],
                             db=dbName,
                             use_unicode=True,
                             charset='utf8mb4',
                             autocommit=True)

    return dbConn

def disconnectFromDatabase(dbConn):
    dbConn.close();

def processFile(fileName,dbConn):
    batchSize = 500
    with open(fileName) as gameFile:
       gameFileReader = csv.DictReader(gameFile)
       batchCounter = 1
       gameList=[]
       for gameDictionary in gameFileReader:
           gameList.append(processRow(dbConn,gameDictionary))
           if (batchCounter == batchSize):
               saveVideoGameBatch(dbConn,gameList)
               gameList = []
               batchCounter = 1
           else:
               batchCounter += 1
    saveVideoGameBatch(dbConn,gameList)


def processRow(dbConn,gameDictionary):
    gameDictionary = cleanDictionary(gameDictionary)
    platformId = getPlatformDimension(dbConn, gameDictionary["Platform"])
    genreId = getGenreDimension(dbConn, gameDictionary["Genre"])
    publisherId = getPublisherDimension(dbConn, gameDictionary["Publisher"], )
    developerId = getDeveloperDimension(dbConn, gameDictionary["Developer"])
    ratingCd = getRatingDimension(dbConn, gameDictionary["Rating"])

    return (
        gameDictionary["Name"], platformId, gameDictionary["Year_of_Release"],
        genreId, publisherId, gameDictionary["NA_Sales"],
        gameDictionary["EU_Sales"], gameDictionary["JP_Sales"], gameDictionary["Other_Sales"],
        gameDictionary["Critic_Score"], gameDictionary["Critic_Count"], gameDictionary["User_Score"],
        gameDictionary["User_Count"], developerId, ratingCd
    )

def cleanDictionary(initialDictionary):
    # Convert empty strings to None in the row dictionary
    # return {key: (value if value != "" else None) for key, value in row.items()}
    cleanDictionary = {}
    for key,value in initialDictionary.items():
        if value == "":
            cleanDictionary[key] = None
        else:
            cleanDictionary[key] = value
    return cleanDictionary


def saveVideoGameBatch(dbConn,batchSQLStatementValues):
    gameInsertSQL= 'INSERT INTO vg_data_fact ' \
                    '(game_name, platform_id,release_year,genre_id,publisher_id,' \
                    'NA_sales,EU_sales,JP_sales,other_sales,' \
                    'critic_score,critic_count,user_score,user_count,developer_id,rating_cd) ' \
                    'VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s);'

    cursor = dbConn.cursor()
    cursor.executemany(gameInsertSQL,batchSQLStatementValues)



def getDimensionId(dbConn,dimensionSQL,dimensionLookupValue,dimensionIdColumn):
    print(f"Running {dimensionSQL}")
    print(f"With Value {dimensionLookupValue}")
    cursor = dbConn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(dimensionSQL,dimensionLookupValue)
    result = cursor.fetchone()
    cursor.close()
    if result is None:
        return None
    return result[dimensionIdColumn]

def insertDimension(dbConn,dimensionInsertSQL,dimensionInsertValue):
    print(f"Running {dimensionInsertSQL}")
    print(f"With Value {dimensionInsertValue}")
    cursor = dbConn.cursor()
    cursor.execute(dimensionInsertSQL,dimensionInsertValue)
    # Get the value of the auto-incremented key
    cursor.execute("SELECT LAST_INSERT_ID()")
    # Fetch the result
    auto_incremented_key = cursor.fetchone()[0]
    cursor.close()
    return auto_incremented_key

def mapSlowDimension(dbConn,dimensionLookupSQL,dimensionInsertSQL,dimensionLookupValue,dimensionIdColumn):
    if dimensionLookupValue is None:
        return None
    dimensionId = getDimensionId(dbConn,dimensionLookupSQL,dimensionLookupValue,dimensionIdColumn)
    if dimensionId is None:
        dimensionId = insertDimension(dbConn,dimensionInsertSQL,dimensionLookupValue)
    return dimensionId

def mapStaticDimension(dbConn,dimensionLookupSQL,dimensionLookupValue,dimensionIdColumn):
    if dimensionLookupValue is None:
        return None
    dimensionId = getDimensionId(dbConn,dimensionLookupSQL,dimensionLookupValue,dimensionIdColumn)
    return dimensionId

def getPlatformDimension(dbConn,platformValue):
    return mapSlowDimension(dbConn, 'select platform_id from vg_platform where platform = %s', 'insert into vg_platform (platform_name) values (%s)', platformValue, 'platform')

def getGenreDimension(dbConn,genreValue):
    return mapSlowDimension(dbConn, 'select genre_id from vg_genre where genre = %s', 'insert into vg_genre (genre_name) values (%s)', genreValue, 'genre')

def getDeveloperDimension(dbConn,developerValue):
    return mapSlowDimension(dbConn, 'select developer_id from vg_developer where developer = %s', 'insert into vg_developer (developer_name) values (%s)', developerValue, 'developer')

def getPublisherDimension(dbConn,publisherValue):
    return mapSlowDimension(dbConn, 'select publisher_id from vg_publisher where publisher = %s', 'insert into vg_publisher(publisher_name) values (%s)', publisherValue, 'publisher')

def getRatingDimension(dbConn,ratingValue):
    return mapStaticDimension(dbConn, 'select rating_id from vg_rating where rating = %s', ratingValue, 'rating')

#Start Main Program
if (__name__ == '__main__'):
    dbSchema = 'ddiaz11'
    configFile = 'credentials.txt'
    dataFile = 'videogamesales-small.csv'
    try:
        #1. Connect To Database
        dbConn = connectToDatabase(dbSchema,configFile)

        try:
            #2 Process the File
            processFile(dataFile,dbConn)

        except Exception as e:
            print(e)
        finally:
            # 3 Close DB Connection
            disconnectFromDatabase(dbConn)
    except Exception as e:
        print(e)