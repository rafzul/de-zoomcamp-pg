import sys
import pandas 

#print arguments
print(sys.argv)

#argument 0 is name of OS file
#argument 1 is actual first argument we care
try:
    day = sys.argv[1]
    print(f'job finished succesfully for day = {day}')
except Exception as e:
    print(e)
    print(f'hayo udh ditulis blm argsnya')
#cool pandas stuff

#print sentence
