import smtplib
from time import sleep
import os
import sys
import time
import json
import requests
import zipfile
#import shutil
import glob
import csv
import mysql.connector
import pypyodbc as pyodbc
from datetime import datetime, timedelta
import re
import ftplib
import pandas as pd
import re
import numpy

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from random import randint


class Detailmis:
	def __init__(self):
		self.findnames = re.compile(r'([A-Z]\w*(?:\s[A-Z]\w*)?)')
		try:
			#self.ftp = ftplib.FTP()
			#self.ftp.connect('180.179.221.76',4022)
			#self.ftp.login('mlacl_smsdlreports_ftp', 'MQdl#5p0rt5acL@o72o20')
			#self.ftp.cwd("ACL/mobiqalt")
			print('')
			
		except Exception as err:
			print(str(err))			
			return

		print ("Ftp Connected")
		time.sleep(1)

	def is_name_in_text(text, names):
	    for possible_name in set(self.findnames.findall(text)):
	        if possible_name in names:
	            return possible_name
	    return False
	def scrap(self,sid,cursor,mycursor,brandid):

		self.sid = sid

		print("in scrap")
		 
		#cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = 'tbl_sender_id'")
		#cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = 'tbl_dns_data '")
		#for row in cursor.fetchall():
		#    print(row)

		self.emailbody = MIMEMultipart('alternative')
		filedirPath = 'D:/wamp64/www/projects/taghash/py/one97/download/'

		ftp = ftplib.FTP()
		ftp.connect('192.168.15.224',4022)
		ftp.login('mltimes_smsdlreports_ftp', 'MQdl#5p0rt5T!mes@122o2o')
		#ftp.cwd("ACL/mobiqalt_july-aug-sep")
		
		filenames = []
		for file_name in ftp.mlsd():
			#findnames[] = file_name[0]
			filenames.append(file_name[0])
		#print(filenames)
		#sys.exit()
		execfiles = []
		path = dir_path = os.path.dirname(os.path.realpath(__file__))
		for filenew in filenames:
			#print(filenew)
			res = list(filter(lambda x:  x in filenew, ['mobiquesttrans_2022-05-24']))
			if res:
				execfiles.append(filenew)
				#### download files ######
				filename = "download/"+filenew
				isfileexist = 0
				if glob.glob(filename):
					#os.remove(filename)
					isfileexist = 1

				if isfileexist == 0:				
					handle = open(path.rstrip("/") + "/download/" + filenew.lstrip("/"), 'wb')
					print("Downloading "+filenew+" ....")			
					ftp.retrbinary('RETR %s' % filenew, handle.write)
					time.sleep(10)
				else :
					time.sleep(3)
		try:
			ftp.quit()
		except Exception as err:
			print("**ftp connection time out**")
			print(str(err))

		for fname in execfiles:
			#print(fname)
			filename = "download/"+fname
			'''isfileexist = 0
			if glob.glob(filename):
				#os.remove(filename)
				isfileexist = 1

			if isfileexist == 0:				
				handle = open(path.rstrip("/") + "/download/" + fname.lstrip("/"), 'wb')
				print("Downloading "+fname+" ....")			
				ftp.retrbinary('RETR %s' % fname, handle.write)
				time.sleep(10)
			else :
				time.sleep(3)'''

			#sys.exit()
			#print(sid)
			time.sleep(6)
			print("now open "+filename)
			with open(filename, 'r', encoding="utf8") as csvfile:
				csvreader = csv.reader(csvfile, delimiter=',')
				print (csvreader)
				#j = 0
				for row in csvreader:
					#print(row)				
					sheetsid = row[1]
					#print(sheetsid)
					#print(sid)
					#if j >=1 :
						
						
					if sheetsid == sid or sheetsid == sid.upper() :
						#print(sheetsid)
						#print(sid)
						#print("in if cond")
						entrytime = row[0]
						#entrytime = entrytime[:-3]
						d = entrytime
						#k = datetime.strptime(d, '%Y-%m-%d %I:%M:%S %p')
						#entrytime = k.strftime('%Y-%m-%d %H:%M:%S')
						#print(entrytime)
						#sys.exit()
						receiver = row[2]
						#content = row[5]
						content = row[3]
						
						cLength = ''
						smstype = ''
						#status = row[3]
						status = row[4]
						dlrtime = entrytime
						#dlrtime = row[0]
						tagname = ''

						#s = "select count(*) from detailedmis where senderid='"+senderid+"'"
						#mycursor.execute(s)
						#myresult = mycursor.fetchall()
						#if len(myresult) < 1:

						#if status != 'D':
						if status != 'DELIVERY_SUCCESS':
							
							#try:
							sq = "INSERT INTO tbl_failed (senderid, receiver, content, dlrtime, status, cpnname) VALUES('"+self.sid+"', '"+receiver+"', '"+re.escape(content.replace("'",'"'))+"', '"+dlrtime+"', '"+re.escape(status.replace("'",'"'))+"', '"+re.escape(tagname.replace("'",'"'))+"')"
							
							cursor.execute(sq)
							conn.commit()
							#sys.exit()
							#except Exception as err:
							#	sq = "INSERT INTO tbl_failed (senderid, receiver, content, dlrtime, status, cpnname) VALUES('"+self.sid+"', '"+receiver+"', '"+re.escape(content.replace("'",'"'))+"', '"+dlrtime+"', '"+re.escape(status.replace("'",'"'))+"', '"+re.escape(tagname.replace("'",'"'))+"')"
							#	print(sq)
							#	print("*** Error on failed record insert process ****")
							#	print(str(err))

						#if status == "D":
						#	statusins = "Delivered"

						#if status == "S":
						#	statusins = "Delivered"

						#if status == "F":
						#	statusins = "Failed"

						#if status == "I":
						#	statusins = "Invalid"

						if status == "DELIVERY_SUCCESS" or status == "DELIVERY_AWAITED":
							statusins = "Delivered"

						if status == "DELIVERY_FAILED" or status == "PSB_GENERIC_ERROR":
							statusins = "Failed"

						if status == "SUBMISSION_REJECTED":
							statusins = "Submission Rejected"

						if status == "":
							if len(receiver) == 10:
								firstdigit = int(str(receiver)[:1])
								if(firstdigit == 6 or firstdigit == 7 or firstdigit == 8 or firstdigit == 9):
									statusins = "Delivered"
								else:
									statusins = "Invalid"
							else:
								statusins = "Invalid"


						sql = "INSERT INTO detailedmis (senderid, entrytime, receiver, content, cLength, smstype, status, dlrtime, tagname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
						#print(sql)
						val = (self.sid, entrytime, receiver, content, cLength, smstype, statusins, dlrtime, tagname)
						mycursor.execute(sql, val)
						mydb.commit()

			   
					#j = j + 1 


	   

		print('Sheet data inserted to db')

		'''try:
			ftp.quit()
		except Exception as err:
			print("**ftp connection time out**")
			print(str(err))'''
		#sys.exit()

		
		cursor.execute("SELECT * from  tbl_dns_data where sender_id= '"+sid+"' and cast(campaign_date as date)='"+yDate+"'")
		rs = []
		rowsn = cursor.fetchall()
		print("Record found in table- tbl_dns_data: ")
		for row in rowsn:        
		   senderid = str(row[3])
		   mobileno = str(row[4])
		   campaigndate = row[5]
		   smstext = row[6]
		   #print(senderid +' '+mobileno+' '+campaigndate)
		   sql = "INSERT INTO detailedmis (senderid, entrytime, receiver, content, status, dlrtime) VALUES (%s, %s, %s, %s, %s, %s)"
		   val = (self.sid, campaigndate, mobileno, smstext, 'Delivered', campaigndate)
		   isins = mycursor.execute(sql, val)
		   #print(isins)
		   #print('sql server query executed')
		   mydb.commit()
		time.sleep(1)

		sql = "select count(*) from detailedmis where senderid='"+sid+"' and entrytime like '"+yDate+"%'"
		mycursor.execute(sql)
		rows = mycursor.fetchall()
		#totalcount = len(rows)
		totalrec = 0
		for ro in rows:
			totalcount =  str(ro[0])
			totalrec = ro[0]
		#print(sql)				
		print('Total count for sid '+sid+' - ' +str(totalcount))

		sql = "update senderids set count='"+str(totalcount)+"' where senderid='"+sid+"' and campaigndate like '"+yDate+"%'"
		mycursor.execute(sql)
		mydb.commit()

		
		filelink = sid+'_'+yDate+'.csv'
		r = requests.get('https://taghash.co/py/one97/exportdb_del_times.php?sid='+sid+'&ydate='+yDate+'&filelink='+filelink, stream = True)
		print("Export to csv line executed")               

		time.sleep(4)
		
		totalfiles = 0
		#if totalrec % 900000 == 0:
		#	totalfiles = totalfiles+1

		#totalrec = 1000000;
		tc = totalrec / 900000 

		if isinstance(tc, int) == False :
			tc = int(tc)+1

		totalfiles = tc	

		print("total files: "+str(totalfiles))

		#sys.exit()
		try:
			self.emailbody['Subject'] = yDate+" Campaign Delivery report for sender id - "+self.sid
			self.emailbody['From'] = "m'loyal SMS Delivery Report<campaignroi@mobiquest.com>"
			self.emailbody['To'] = "mloyalsupport@mloyal.com"

			sdlinks = ''
			#for fc in range(totalfiles+1):
			for fc in range(totalfiles):
				mainfile = sid+'_times_'+yDate+'_'+str(fc)+'.csv'				
				encfile = sid+'_times_m_'+yDate+'_'+str(fc)+'.csv'
				if sid == 'CTYKRT':
					dlink = "http://taghash.co/py/one97/exports/"+mainfile
				else :
					dlink = "http://taghash.co/py/one97/exports/"+encfile

				
				response = requests.get("http://mqml.co/shorten/index.php?keyword="+self.sid+str(randint(1000, 9999))+"&url="+dlink+"&title=Delivery report&ip=180.179.198.149")
				response.encoding = 'utf-8'
				shortLink = response.text
				print(shortLink)
				sdlinks = sdlinks + "<br>"
				sdlinks = sdlinks + shortLink
				#sys.exit()

			self.msg = "Please find campaign delivery report for campaigns done this month.Please download report from here- <br>"+sdlinks
			print(self.msg)
			#sys.exit()
			print("mail body is below --")
			print(self.msg)

			# Record the MIME types of both parts - text/plain and text/html.

			part2 = MIMEText(self.msg, 'html')
			#print(2)

			#self.emailbody.attach(self.msg)
			self.emailbody.attach(part2)


			server = smtplib.SMTP('smtp.gmail.com', 587)
			#print(7)

			server.starttls()
							
			#server.login("anoop@paytmmloyal.com", "mobianoop@1972")
			server.login("campaignroi@mobiquest.com", "c@mp@ign@2018")

			orderJson = { 
				"objClass": {
						"bid": brandid
					}
				}

			headers = {'Content-Type': 'application/json','userid':'mob_usr','pwd':'81392313-06D4-408A-ABAD-8201DE785F12',"Content-Length": str(len(orderJson)),"Accept": "*/*"}       

			r = requests.post("http://livemonitorapi.mloyalcapture.com/service.svc/GET_EMAIL_DETAIL", data=json.dumps(orderJson), headers=headers)    
			#r.encoding = 'utf-8'
			rText = json.loads(r.text)
			if rText['GET_EMAIL_DETAILResult']['Success'] == 1:
				emails = rText['GET_EMAIL_DETAILResult']['email_string']
				toaddr = emails.split(',')
				#toaddr.append("prashant@paytmmloyal.com")
				#toaddr.append("vineet@paytmmloyal.com")
				#toaddr.append("anoop@paytmmloyal.com")
				#toaddr.append("neeraj@paytmmloyal.com")
				toaddr.append("ankit.bhatt@paytmmloyal.com")
				# toaddr.append("pankaj@paytmmloyal.com")
				#toaddr.append("neeraj@paytmmloyal.com")
				
				
			else:
				print('Email ids not found')
			
			if sid == 'BSKRBN':
				toaddr = ['garima@paytmmloyal.com', 'deepika.kumari@paytmmloyal.com', 'komal@paytmmloyal.com','vinay@paytmmloyal.com','ankit.bhatt@paytmmloyal.com']

					
			#toaddr = ["neeraj@paytmmloyal.com","ankit.bhatt@paytmmloyal.com"]

			if sid == 'INOXMV':
				toaddr = ['ankit.bhatt@paytmmloyal.com','deepika.kumari@paytmmloyal.com']

			print(toaddr)
			#server.sendmail("anoop@paytmmloyal.com", toaddr , self.emailbody.as_string())
			server.sendmail("m'loyal Deliver Report<campaignroi@mobiquest.com>", toaddr , self.emailbody.as_string())
			print("email line executed")
			server.quit()
			#print(9)

			#delete record form database
			try:
				sqld = "delete from detailedmis where senderid='"+self.sid+"' and entrytime like '"+yDate+"%'"
				mycursor.execute(sqld)
				mydb.commit()
				print("record deleted from databse for sid = "+str(self.sid)+" for date - "+yDate)
			except Exception as err:
				print("*** Unable to delete record from database ****")
				print(str(err))	

			# Store cussess senderid
			successfile = 'logs/sent_senderid_'+yDate+'.txt'
			if glob.glob(successfile):
				print("")
			else :  
				open(str(successfile), 'w+', encoding="utf-8")
				
			appendresponse = open(successfile, "a")
			# write line to output file
			#appendresponse.write(r.content+',,,, order id - '+billno)
			appendresponse.write(str(sid)+' - '+str(totalcount)+',<br>')
			appendresponse.write("\n")
			appendresponse.close()
			print('success sid written on file')
				
		except Exception as err:
			print("*** email not sent ****")
			print(str(err))
			#driver.quit()
			return

		#if glob.glob(filedirPath+filename):
		#	os.remove(filedirPath+filename)
		'''				
		except Exception as err:
			print("*** SID not found in mis portal ****")
			responseFile = 'logs/error_on_senderid_'+yDate+'.txt'
			if glob.glob(responseFile):
				print("")
			else :  
				open(str(responseFile), 'w+', encoding="utf-8")
				
			appendresponse = open(responseFile, "a")
			# write line to output file
			#appendresponse.write(r.content+',,,, order id - '+billno)
			appendresponse.write(str(sid))
			appendresponse.write("\n")
			appendresponse.close()
			return
			'''
		

if __name__ == '__main__':
	
	mis = Detailmis()
	#conn = pyodbc.connect('Driver={SQL Server};Server=13.71.80.78,2499;Database=DNS;UID=mqphpsqldb;PWD=M0B1PhPcsgmNtDB@2o!8;Trusted_Connection=yes;')
	#sql server connection
	#conn = pyodbc.connect('Driver={SQL Server};Server=180.179.202.114,2499;Database=DNS;UID=mappsdb;PWD=MappsSuPerbsqlDB@042o!8;Trusted_Connection=no;')
	conn = pyodbc.connect('Driver={SQL Server};Server=192.168.15.98,2499;Database=DNS;UID=mappsdb;PWD=MappsSuPerbsqlDB@042o!8;Trusted_Connection=no;')

	cursor = conn.cursor()

	# Mysql connection
	mydb = mysql.connector.connect(
	  host="localhost",
	  user="root",
	  passwd="mLoyalmysql@2019",
	  database="mis"
	)
	mycursor = mydb.cursor()



	yDate = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
	yDate = '2022-05-24'
	cursor.execute("SELECT * from  tbl_sender_id where sms_api = 'Times' and cast(campaign_date as date)='"+yDate+"'")
	#cursor.execute("SELECT * from  tbl_sender_id where cast(campaign_date as datetime)='2019-08-13'")
	#cursor.execute("SELECT * from  tbl_sender_id where sender_id= 'amante' and cast(campaign_date as datetime)='"+yDate+"'")
	rs = []
	#for row in cursor.fetchall(): 
	#    print(row)
	#sys.exit()    
	rows = cursor.fetchall()
	#print(len(rows))
	#sys.exit()
	if(len(rows) > 0):

		for row in rows:   
			sql = "INSERT INTO senderids (senderid, campaigndate) VALUES (%s, %s)"
			val = (row[3], row[4])
			mycursor.execute(sql, val)
			mydb.commit()

			brandid = row[1]
			#print(row)        
			senderid = row[3]

			
			#if (senderid != 'amante') and (senderid != 'COSTAc') and (senderid != 'GTNJLI') and (senderid != 'aromed') and (senderid != 'PandVKA') and (senderid != 'LAMODE') and (senderid != 'DENOVO') and (senderid != 'MINTHM') and (senderid != 'CTYKRT') and (senderid != 'PRTRON') and (senderid != 'PORVKA') : #and (senderid != 'BOMBAY'):
			
			try:
				#if (senderid != 'amante'):
				sqld = "delete from detailedmis"
				mycursor.execute(sqld)
				mydb.commit()

				#if ((senderid == 'COSTAc') or (senderid == 'JSTCAS') or (senderid == 'SPCMAK') and (senderid == 'PIKKLE') and (senderid == 'MeNMom') and (senderid == 'PLEJFT') and (senderid == 'MaatiC')  and (senderid == 'RANNAG') and (senderid == 'TMZONE') and (senderid == 'iHOCCO')):
				#if senderid == 'NIVAAN' or senderid == 'IOSISS' or senderid == 'JPEARL' or senderid == 'BAWAHT' or senderid == 'RatanJ' or senderid == 'Orchid' or senderid == 'PECOSP' or senderid == 'AVIRATE':					
				#if senderid != 'KVNTRS' or senderid != 'MeNMom' or senderid != 'Mqprom' or senderid != 'Numero' or senderid != 'REGALs' :
				if (senderid != 'DOMINO') and (senderid != 'Mqloyl') and (senderid != 'mqloyl') :
					#if senderid != 'PORVIK' and senderid != 'PORVKA':
					#if senderid == 'CosmoB' or senderid == 'SODAOP' or senderid == 'RCSHOE' or senderid == 'SxCIAL':					
					print('Process started for sender id - '+senderid)
					mis.scrap(senderid,cursor,mycursor,brandid)
					print('Process completed for sender id - '+senderid) 
					#sys.exit() 

			except Exception as err:
				print(str(err))
				responseFile = 'logs/error_on_senderid_'+yDate+'.txt'
				if glob.glob(responseFile):
					print("")
				else :  
					open(str(responseFile), 'w+', encoding="utf-8")
					
				appendresponse = open(responseFile, "a")
				# write line to output file
				#appendresponse.write(r.content+',,,, order id - '+billno)
				appendresponse.write(str(senderid))
				appendresponse.write("\n")
				appendresponse.close()

			#sys.exit()

		# Send email for sent sender ids
		
		#sys.exit()
		
		emailbody = MIMEMultipart('alternative')
		emailbody['Subject'] = yDate+" Times SMS Delivery Sent/Failed sender ids"
		emailbody['From'] = "m'loyal SMS Delivery Report<campaignroi@mobiquest.com>"
		emailbody['To'] = "mloyalsupport@mloyal.com"
		#emailbody['To'] = "neeraj@paytmmloyal.com"

		#filelink = 'GTNJLI_2019-08-18.csv'
		

		slink = 'http://taghash.co/py/one97/logs/sent_senderid_'+yDate+'.txt'
		response = requests.get(slink)
		response.encoding = 'utf-8'
		shortLink1 = response.text
		
		msg = "Please find Times sms delivery Successfully sent sender ids list. <br>"+shortLink1
		
		#print(1)
		print("mail 2 body is below --")
		print(msg)

		part2 = MIMEText(msg, 'html')
		#print(2)

		#emailbody.attach(msg)
		emailbody.attach(part2)


		server = smtplib.SMTP('smtp.gmail.com', 587)
		#print(7)

		server.starttls()

		toaddr=["anoop@paytmmloyal.com","vineet@paytmmloyal.com","neeraj@paytmmloyal.com"]
		#toaddr=["anoop@paytmmloyal.com","neeraj@paytmmloyal.com"]
						
		#server.login("anoop@paytmmloyal.com", "mobianoop@1972")
		server.login("campaignroi@mobiquest.com", "c@mp@ign@2018")

		server.sendmail("m'loyal Deliver Report<campaignroi@mobiquest.com>", toaddr , emailbody.as_string())
		print("email line executed")
		server.quit()


		#Failed sender ids
		

		if glob.glob('logs/error_on_senderid_'+yDate+'.txt'):
			emailbody = MIMEMultipart('alternative')
			emailbody['Subject'] = yDate+" Times SMS Delivery Sent/Failed sender ids"
			emailbody['From'] = "m'loyal SMS Delivery Report<campaignroi@mobiquest.com>"
			emailbody['To'] = "mloyalsupport@mloyal.com"
			#emailbody['To'] = "neeraj@paytmmloyal.com"

			#filelink = 'GTNJLI_2019-08-18.csv'
			
			elink = 'http://taghash.co/py/one97/logs/error_on_senderid_'+yDate+'.txt'
			response = requests.get(elink)
			response.encoding = 'utf-8'
			shortLink = response.text
			
			msg = "<br>Please find sms delivery Failed sender ids list. <br>"+shortLink
			#print(1)
			print("mail 2 body is below --")
			#print(msg)

			part2 = MIMEText(msg, 'html')
			#print(2)

			#emailbody.attach(msg)
			emailbody.attach(part2)


			server = smtplib.SMTP('smtp.gmail.com', 587)
			#print(7)

			server.starttls()

			toaddr=["anoop@paytmmloyal.com","neeraj@paytmmloyal.com"]
							
			#server.login("anoop@paytmmloyal.com", "mobianoop@1972")
			server.login("campaignroi@mobiquest.com", "c@mp@ign@2018")

			server.sendmail("m'loyal Deliver Report<campaignroi@mobiquest.com>", toaddr , emailbody.as_string())
			print("email line executed")
			server.quit()
		else :
			print("Failed file not found")
		
		
		


		

		
	else:
		print('No sender id found!')
	#driver.quit()


