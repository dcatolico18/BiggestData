import csv
import datetime

class SimpleData():

    def writeToFile(self, inputFilename, outputFilename):
        with open(inputFilename) as inputFile:
            with open(outputFilename, "w+") as outputFile:
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
                    timeOfDay[1] = self.myround(timeOfDay[1])
                    timeOfDay = sum(timeOfDay)
                    travelTime = self.myround(int(words[4].split('.')[0]), 5)
                    dataWriter.writerow([route, tollgate, dayOfWeek, timeOfDay, travelTime])

    def myround(self, x, base=20):
        return int(base * round(float(x)/base))