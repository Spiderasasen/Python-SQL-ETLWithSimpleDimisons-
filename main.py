# from importlib.metadata import pass_none

import pymysql
import configparser
import csv

#getting the credentials to connect the database
config = configparser.ConfigParser()
config.read_file(open('credentails.txt'))
dbhost = config['csc']['dbhost']
dbuser = config['csc']['dbuser']
dbpw = config['csc']['dbpw']


#choses the schema
dbschema = 'ddiaz11'

#opens the database connection
dbconn = pymysql.connect(host=dbhost,
                         user=dbuser,
                         passwd=dbpw,
                         db=dbschema,
                         use_unicode=True,
                         charset='utf8mb4',
                         autocommit=True)
cursor = dbconn.cursor()


insert_query: str = 'insert into video_game_data(name, platform, release_year, genre, publisher, NA_sales, EU_sales, JP_sales, critic_score, critic_count, user_score, developer, rating) values (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)'
rows_insterted = []

filename = 'videogamesales-small.csv'
try:
   with open(filename) as game_file:
      game_file_reader = csv.DictReader(game_file)
      for game_dictionary in game_file_reader:
        rows_insterted.append((game_dictionary['Name'],
                          game_dictionary['Platform'],
                          game_dictionary['Year_of_Release'],
                          game_dictionary['Genre'],
                          game_dictionary['NA_Sales'],
                               game_dictionary['EU_Sales'],
                               game_dictionary['JP_Sales'],
                               game_dictionary['Critic_Score'],
                               game_dictionary['Critic_Count'],
                               game_dictionary['User_Score'],
                               game_dictionary['Developer'],
                               game_dictionary['Rating']))
      cursor.executemany(insert_query, rows_insterted)
except Exception as e:
   print(f"Error Occurred: {e}")

#setting up a queury as a string
# selectCountryCount = "SELECT count(Code) FROM Country;"
# countryName: str = "Z%"
# multipleResults = f"select Code, CountryName from Country where CountryName like '{countryName}';"


#execute qury
# cursor.execute(selectCountryCount)


# Get the result for query
# result = cursor.fetchone() #grabs one
# result = cursor.fetchall() #grabs all
# result = cursor.fetchall() #grabs the first 10 sections

# Do something with result
# NOTE: the [0] says to access the first (and ONLY in this case) column from our SQL statement
# for row in result:
#     print('Code:', row[0], end=' -> ')
#     print('Country Name:', row[1])


# Closing connection
dbconn.close()
