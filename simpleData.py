import csv
import datetime

class SimpleData():

    def writeToFile(self):
        with open('trajectories(table 5)_training.csv') as inputFile:
            with open('output.csv', "w+") as outputFile:
                dataWriter = csv.writer(outputFile)
                for index, line in enumerate(inputFile):
                    if index == 0:
                        continue
                    words = line.split(' ')
                    words = words[0].split(",") + words[1].split(",")
                    route = ord(words[0]) - 65
                    tollgate = int(words[1])
                    dateList = words[2].split("/")
                    lineDate = datetime.date(2000 + int(dateList[2]), int(dateList[0]), int(dateList[1]))
                    dayOfWeek = lineDate.weekday()
                    timeOfDay = list(map(int, words[3].split(":")))
                    timeOfDay[0] *= 60
                    timeOfDay = sum(timeOfDay)
                    travelTime = int(words[4].split('.')[0])
                    dataWriter.writerow([route, tollgate, dayOfWeek, timeOfDay, travelTime])
