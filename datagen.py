from faker import Faker
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
import multiprocessing
from multiprocessing import Process
import uuid
import math
import os

data_dir = '/data'

#total records number
records = 100000000

#leave one core for system operations
ncpu = multiprocessing.cpu_count()-1

#records per process
rpp = math.floor(records / ncpu)

#load schema from data.avsc
schema = avro.schema.parse(open('data.avsc', 'rb').read())

#generate localized fake data
fake = Faker('zh_CN')

def gen(pid, lines):
	data_file = os.path.join(data_dir, 'data-part{}.avro'.format(pid))
	writer = DataFileWriter(open(data_file, 'wb'), DatumWriter(), schema)
	for i in range(lines):
		data = {
				'uid':			str(uuid.uuid4()),
				'username':		fake.user_name(),
				'age':			fake.random_int(min=18, max=90),
				'name':			fake.name(),
				'address':		fake.address(),
				'city':			fake.city(),
				'email':		fake.email(),
				'register_ip':	fake.ipv4(),
				'mac':			fake.mac_address(),
				'phone':		fake.phone_number(),
				'ssn':			fake.ssn()
			}
		writer.append(data)
		del data
		#print progress every 10000 records generated
		if i%10000 == 0:
			print('[{}] : {} lines generated.'.format(pid, i))
	writer.close()
	print('[{}] : done.'.format(pid))

#spawn processes
plist = []
for i in range(ncpu):
	#last partition
	if rpp * (i+2) > records:
		rpp = records - i*rpp
	p = Process(target=gen, args=(i+1,rpp))
	plist.append(p)
	#bind to specific core
	os.system('taskset -p -c {} {}'.format(i+1, p.pid))
	p.start()

for p in plist:
	p.join()