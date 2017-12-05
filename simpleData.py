import csv
import datetime
import weather

class SimpleData():

    def __init__(self):
        self.rainDict = weather.Weather()

    def writeToFile(self, inputFilename, outputFilename):
        with open(inputFilename) as inputFile:
            with open(outputFilename, "w+", newline='') as outputFile:
                dataWriter = csv.writer(outputFile)
                for index, line in enumerate(inputFile):
                    if index == 0:
                        continue
                    words = self.getWords(line)
                    route = self.getRoute(words)
                    tollgate = self.getTollgate(words)
                    dayOfWeek = self.getDayOfWeek(words)
                    startTime = self.getStartTime(words)
                    travelTime = self.getTravelTime(words)
                    rain = self.getRain(words)
                    dataWriter.writerow([route, tollgate, dayOfWeek, startTime, rain, travelTime])

    def myround(self, x, base=20):
        return int(base * round(float(x)/base))

    def getWords(self, line):
        splitLine = line.split(' ')
        return splitLine[0].split(",") + splitLine[1].split(",")

    def getRoute(self, words):
        return (ord(words[0]) - 65)/3.0

    def getTollgate(self, words):
        return int(words[1])/3.0

    def getDayOfWeek(self, words):
        dateList = words[2].split("/")
        lineDate = datetime.date(2000 + int(dateList[2]), int(dateList[0]), int(dateList[1]))
        return lineDate.weekday()/6.0

    def getStartTime(self, words):
        timeOfDay = list(map(int, words[3].split(":")))
        timeOfDay[0] *= 60
        timeOfDay[1] = self.myround(timeOfDay[1])
        return sum(timeOfDay)/1440.0

    def getTravelTime(self, words):
        return self.myround(int(words[4].split('.')[0]), 10)

    def getRain(self, words):
        dateList = words[2].split("/")
        lineDate = datetime.date(2000 + int(dateList[2]), int(dateList[0]), int(dateList[1]))
        return self.rainDict.lookupTable[lineDate]