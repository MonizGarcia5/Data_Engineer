#Prof. Fernando Amaral
import time
import re
import datetime
from kafka import KafkaProducer as kp

arquivo = open(r'/var/log/apache2/access.log','r')
regexp = '^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\"'
produtor = kp(bootstrap_servers="127.0.0.1:9092")
while 1:
	linha = arquivo.readline()
	if not linha:
		time.sleep(5)
	else:
		x = re.match(regexp, linha).groups()
		msg = bytes(str(x), encoding='ascii')
		produtor.send("apachelog", msg)
		print("Mensagem enviada em ", datetime.datetime.now())
