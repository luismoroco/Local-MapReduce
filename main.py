from src.Framework.MapReduceFramework import MapReduceFramework
from src.Strategy.Map import EmmiterTextCountMapper
from src.Strategy.Reduce import TextCounterReducer

import logging as logg

logg.basicConfig(level=logg.DEBUG, format='%(threadName)s: %(message)s')

mapReduce = MapReduceFramework(
   EmmiterTextCountMapper(), 
   TextCounterReducer(),
   12)

mapReduce.map()
mapReduce.reduce()

print(mapReduce.getResults())