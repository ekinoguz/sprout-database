import random

# Read names file
# Create random age, height, salary
# Save employee file as name,age,height,salary
def employee():
	f = open('names', 'r')
	lines = f.readlines()
	out = ''
	index = 0
	for line in lines:
		age = random.randint(18,70)
		height = random.randint(55, 71)/10.0
		salary = random.randint(50, 250) * 1008
		out += line[:-1] + ',' + str(age) + ',' + str(height)  + ',' + str(salary) + '\n'
	f.close()
	f = open('employee', 'w')
	f.write(out)
	print out

employee();