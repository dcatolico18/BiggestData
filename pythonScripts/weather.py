import csv
from collections import defaultdict
import datetime

class Weather():

    def __init__(self):
        self.lookupTable = defaultdict(float)
        self.scrape()

    def scrape(self):
        with open("dataSets/training/weather (table 7)_training.csv") as csvfile:
            weatherReader = csv.reader(csvfile)
            for i, row in enumerate(weatherReader):
                if i == 0:
                    continue
                rowDate = row[0].split("-")
                lineDate = datetime.date(int(rowDate[0]), int(rowDate[1]), int(rowDate[2]))
                rain = float(row[8])
                self.lookupTable[lineDate] += rain

        totalRain = 0
        for key in self.lookupTable.keys():
            totalRain += self.lookupTable[key]

        largestRain = max([i for i in self.lookupTable.values()])
        for key in self.lookupTable.keys():
            self.lookupTable[key] /= largestRain