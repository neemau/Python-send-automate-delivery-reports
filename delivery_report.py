#Developer : Neeraj maurya
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
			#self.ftp.connect('FTP_HOST',PORT)
			#self.ftp.login('FTP_LOGIN', 'FTP_PWD')
			#self.ftp.cwd("dir1/dir2")
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
		 
		#cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = 'tablename'")
		#cursor.execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_NAME = 'tablename '")
		#for row in cursor.fetchall():
		#    print(row)

		self.emailbody = MIMEMultipart('alternative')
		filedirPath = 'D:/wamp64/www/projects/dir/py/dir/download/'

		ftp = ftplib.FTP()
		ftp.connect('FTP_HOST','PORT')
		ftp.login('FTP_LOGIN', 'FTP_PWD')
		#ftp.cwd("dir1/dir3")
		
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
			res = list(filter(lambda x:  x in filenew, ['CSV_FILE_NAME']))
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

						#s = "select count(*) from table_name where senderid='"+senderid+"'"
						#mycursor.execute(s)
						#myresult = mycursor.fetchall()
						#if len(myresult) < 1:

						#if status != 'D':
						if status != 'DELIVERY_SUCCESS':
							
							#try:
							sq = "INSERT INTO table_name (senderid, receiver, content, dlrtime, status, cpnname) VALUES('"+self.sid+"', '"+receiver+"', '"+re.escape(content.replace("'",'"'))+"', '"+dlrtime+"', '"+re.escape(status.replace("'",'"'))+"', '"+re.escape(tagname.replace("'",'"'))+"')"
							
							cursor.execute(sq)
							conn.commit()
							#sys.exit()
							#except Exception as err:
							#	sq = "INSERT INTO tbl_failed (senderid, receiver, content, dlrtime, status, cpnname) VALUES('"+self.sid+"', '"+receiver+"', '"+re.escape(content.replace("'",'"'))+"', '"+dlrtime+"', '"+re.escape(status.replace("'",'"'))+"', '"+re.escape(tagname.replace("'",'"'))+"')"
							#	print(sq)
							#	print("*** Error on failed record insert process ****")
							#	print(str(err))

						

						sql = "INSERT INTO table_name (senderid, entrytime, receiver, content, cLength, smstype, status, dlrtime, tagname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
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

		
		cursor.execute("SELECT * from  db_table where sender_id= '"+sid+"' and cast(column_name as date)='"+yDate+"'")
		rs = []
		rowsn = cursor.fetchall()
		print("Record found in table- tbl_dns_data: ")
		for row in rowsn:        
		   senderid = str(row[3])
		   mobileno = str(row[4])
		   campaigndate = row[5]
		   smstext = row[6]
		   #print(senderid +' '+mobileno+' '+campaigndate)
		   sql = "INSERT INTO table_name (senderid, entrytime, receiver, content, status, dlrtime) VALUES (%s, %s, %s, %s, %s, %s)"
		   val = (self.sid, campaigndate, mobileno, smstext, 'Delivered', campaigndate)
		   isins = mycursor.execute(sql, val)
		   #print(isins)
		   #print('sql server query executed')
		   mydb.commit()
		time.sleep(1)

		sql = "select count(*) from table_name where senderid='"+sid+"' and entrytime like '"+yDate+"%'"
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
		r = requests.get('https://HOST/py/dir/exportdb_del_times.php?sid='+sid+'&ydate='+yDate+'&filelink='+filelink, stream = True)
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
			self.emailbody['Subject'] = yDate+"  Delivery report for sender id - "+self.sid
			self.emailbody['From'] = "<FROM_EMAIL_ID>"
			self.emailbody['To'] = "EMAIL_ID"

			sdlinks = ''
			#for fc in range(totalfiles+1):
			for fc in range(totalfiles):
				mainfile = sid+'_times_'+yDate+'_'+str(fc)+'.csv'				
				encfile = sid+'_times_m_'+yDate+'_'+str(fc)+'.csv'
				if sid == 'XXXXX':
					dlink = "http://HOST/py/dir/exports/"+mainfile
				else :
					dlink = "http://HOST/py/dir/exports/"+encfile

				
				response = requests.get("http://URL_SHORT_HOST/shorten/index.php?keyword="+self.sid+str(randint(1000, 9999))+"&url="+dlink+"&title=Delivery report&ip=180.179.198.149")
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
							)
			server.login("SMTP_USER_NAME", "SMTP_PWD")

			orderJson = { 
				"objClass": {
						"bid": brandid
					}
				}

			headers = {'Content-Type': 'application/json','userid':'userid','pwd':'pwd',"Content-Length": str(len(orderJson)),"Accept": "*/*"}       

			r = requests.post("http://HOST/service.svc/GET_EMAIL_DETAIL", data=json.dumps(orderJson), headers=headers)    
			#r.encoding = 'utf-8'
			rText = json.loads(r.text)
			if rText['GET_EMAIL_Result']['Success'] == 1:
				emails = rText['GET_EMAIL_Result']['email_string']
				toaddr = emails.split(',')
				
				toaddr.append("EMAIL_ID")
				
				
				
			else:
				print('Email ids not found')

			print(toaddr)
			server.sendmail("<TO_EMAIL>", toaddr , self.emailbody.as_string())
			print("email line executed")
			server.quit()
			#print(9)

			#delete record form database
			try:
				sqld = "delete from tblname where senderid='"+self.sid+"' and entrytime like '"+yDate+"%'"
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
	#conn = pyodbc.connect('Driver={SQL Server};Server=HOST,PORT;Database=DBNAME;UID=UID;PWD=PWD;Trusted_Connection=yes;')
	#sql server connection
	#conn = pyodbc.connect('Driver={SQL Server};Server=HOST,PORT;Database=DBNAME;UID=UID;PWD=PWD;Trusted_Connection=no;')
	conn = pyodbc.connect('Driver={SQL Server};Server=HOST,PORT;Database=DBNAME;UID=UID;PWD=PWD;Trusted_Connection=no;')

	cursor = conn.cursor()

	# Mysql connection
	mydb = mysql.connector.connect(
	  host="HOST",
	  user="USERNAME",
	  passwd="PWD",
	  database="DBNAME"
	)
	mycursor = mydb.cursor()



	yDate = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
	yDate = '2022-05-24'
	cursor.execute("SELECT * from  table_name where sms_api = 'Times' and cast(campaign_date as date)='"+yDate+"'")
	rs = []
	#for row in cursor.fetchall(): 
	#    print(row)
	#sys.exit()    
	rows = cursor.fetchall()
	#print(len(rows))
	#sys.exit()
	if(len(rows) > 0):

		for row in rows:   
			sql = "INSERT INTO table_NAME (senderid, campaigndate) VALUES (%s, %s)"
			val = (row[3], row[4])
			mycursor.execute(sql, val)
			mydb.commit()

			brandid = row[1]
			#print(row)        
			senderid = row[3]

			
		
			try:
				
				sqld = "delete from table_NAME"
				mycursor.execute(sqld)
				mydb.commit()

				
								
				print('Process started for sender id - '+senderid)
				mis.scrap(senderid,cursor,mycursor,brandid)
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
		emailbody['Subject'] = MAIL SUBJECT"
		emailbody['From'] = "EMAIL_FROM"
		emailbody['To'] = "TO_EMAIL"

	

		slink = 'http://HOST/py/dir/logs/sent_senderid_'+yDate+'.txt'
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

		toaddr=["TO_EMAIL1","TO_EMAIL2"]
		
						
		server.login("SMTP_USER_NAME", "SMTP_PWD")

		server.sendmail("<EMAIL_ID>", toaddr , emailbody.as_string())
		print("email line executed")
		server.quit()


		#Failed sender ids
		

		if glob.glob('logs/error_on_senderid_'+yDate+'.txt'):
			emailbody = MIMEMultipart('alternative')
			emailbody['Subject'] = MAIL SUBJECT"
			emailbody['From'] = EMAIL_FROM"
			emailbody['To'] = "TO_EMAIL"

			
			elink = 'http://HOST/py/dir/logs/error_on_senderid_'+yDate+'.txt'
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

			toaddr=["TO_EMAIL1","TO_EMAIL2"]
			
			server.login("SMTP_USER_NAME", "SMTP_PWD")

			server.sendmail("TO_MAIL_ID", toaddr , emailbody.as_string())
			print("email line executed")
			server.quit()
		else :
			print("Failed file not found")
		
		
		


		

		
	else:
		print('No sender id found!')
	#driver.quit()


