import csv
import datetime
import weather

class SimpleData():

    def __init__(self):
        self.rainDict = weather.Weather()
        self.dataRDD

    def writeToFile(self, inputFilename, outputFilename):
        with open(inputFilename) as inputFile:
            with open(outputFilename, "w+", newline='') as outputFile:
                dataWriter = csv.writer(outputFile)
                for index, line in enumerate(inputFile):
                    if index == 0:
                        continue
                    words = self.getWords(line)
                    routes = self.getRoute(words)
                    tollgates = self.getTollgate(words)
                    dayOfWeek = self.getDayOfWeek(words)
                    startTime = self.getStartTime(words)
                    travelTime = self.getTravelTime(words)
                    rain = self.getRain(words)
                    dataWriter.writerow(routes + tollgates + dayOfWeek + [startTime, rain, travelTime])


    def writeToRDD(self, inputFileName):
        sc = pyspark.SparkContext(master="local")
        self.dataRDD = sc.textFile(inputFileName)
        with open(inputFilename) as inputFile:
            for index, line in enumerate(inputFile):
                if index == 0:
                    continue
                words = self.getWords(line)
                routes = self.getRoute(words)
                tollgates = self.getTollgate(words)
                dayOfWeek = self.getDayOfWeek(words)
                startTime = self.getStartTime(words)
                travelTime = self.getTravelTime(words)
                rain = self.getRain(words)
                self.dataRDD = self.dataRDD.union(routes + tollgates + dayOfWeek + [startTime, rain, travelTime])
        return self.dataRDD

    def myround(self, x, base=20):
        return int(base * round(float(x)/base))

    def getWords(self, line):
        splitLine = line.split(' ')
        return splitLine[0].split(",") + splitLine[1].split(",")

    def getRoute(self, words):
        routeList = [0, 0, 0]
        routeList[ord(words[0]) - 65] += 1
        return routeList

    def getTollgate(self, words):
        tollgates = [0, 0, 0]
        tollgates[int(words[1]) - 1] += 1
        return tollgates

    def getDayOfWeek(self, words):
        daysOfWeek = [0,0,0,0,0,0,0]
        dateList = words[2].split("/")
        lineDate = datetime.date(2000 + int(dateList[2]), int(dateList[0]), int(dateList[1]))
        daysOfWeek[lineDate.weekday()] += 1
        return daysOfWeek

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

def main():
    try:

        # load the training and test data set
        inputFile = input('Enter name of input file : ')
        outputFile = input('Enter name of output file : ')

        scrapedData = SimpleData()

        scrapedData.writeToFile(inputFile, outputFile)

    except ValueError as v:
        print(v)

    except FileNotFoundError:
        print('File not found')


if __name__ == '__main__':
    main()
