import requests, time

#startTime = time.time() 

iterationNumber = 1
while 1 == 1:
    time.sleep(2)

    text = 'Python test message %i' % iterationNumber
    r = requests.post('http://localhost:5000/exchange', data = text)
    
    print(text)

    iterationNumber = iterationNumber + 1 

#finishTime = time.time() 
#resultSeconds = finishTime - startTime
#print('The receiving has took %i seconds' % resultSeconds)