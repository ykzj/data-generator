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
import sys

data_dir = '.'

#total records number
records = 100

#leave one core for system operations
ncpu = multiprocessing.cpu_count()-1

#records per process
rpp = math.floor(records / ncpu)

#load schema from data.avsc
schema = avro.schema.parse(open('data.avsc', 'rb').read())

#generate localized fake data
fake = Faker('zh_CN')

def avro_thread(pid, lines):
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

def csv_thread(pid, lines):
	data_file = os.path.join(data_dir, 'data-part{}.csv'.format(pid))
	with open(data_file, 'a') as fp:
		writer = csv.writer(fp)
		header = [
			'uid', 'username', 'age', 'name', 'address', 'city', 'email','register_ip', 'mac', 'phone', 'ssn'
		]
		writer.writerow(header)
		for i in range(lines):
			data = [
					str(uuid.uuid4()),
					fake.user_name(),
					fake.random_int(min=18, max=90),
					fake.name(),
					fake.address(),
					fake.city(),
					fake.email(),
					fake.ipv4(),
					fake.mac_address(),
					fake.phone_number(),
					fake.ssn()
				]
			writer.writerow(data)
			del data
			#print progress every 10000 records generated
			if i%10000 == 0:
				print('[{}] : {} lines generated.'.format(pid, i))
	print('[{}] : done.'.format(pid))

def gen_avro(rpp):
	#spawn processes
	plist = []
	for i in range(ncpu):
		#last partition
		if rpp * (i+2) > records:
			rpp = records - i*rpp
		p = Process(target=avro_thread, args=(i+1,rpp))
		plist.append(p)
		#bind to specific core
		p.start()
		os.system('taskset -p -c {} {}'.format(i+1, p.pid))
		
	for p in plist:
		p.join()

def gen_csv(rpp):
	#spawn processes
	plist = []
	for i in range(ncpu):
		#last partition
		if rpp * (i+2) > records:
			rpp = records - i*rpp
		p = Process(target=csv_thread, args=(i+1,rpp))
		plist.append(p)
		#bind to specific core
		p.start()
		os.system('taskset -p -c {} {}'.format(i+1, p.pid))
		
	for p in plist:
		p.join()

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print('usage: python {} avro|csv'.format(sys.argv[0]))
		sys.exit(2)
	if sys.argv[1] == 'avro':
		gen_avro(rpp)
	elif sys.argv[1] == 'csv':
		gen_csv(rpp)
	else:
		print('usage: python {} avro|csv'.format(sys.argv[0]))
		sys.exit(2)


