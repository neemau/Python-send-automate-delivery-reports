# Python-send-automate-delivery-reports
Here an example of sending automate delivery reports on daily basis for multiple brands.

What we do --
Create mysql connection.
Create sql server connection.

Connect to ftp using below code -- 

ftp = ftplib.FTP()
ftp.connect('FTP HOST', PORT)
ftp.login('FTP_USER', 'FTP_PWD')

Then Download all the csv files by name for the given date.
 Check - skip download if already downloaded (because code is in loop)
 Now read the csv file brand wise -  
 Insert specific conditioned data into mysql database
 
 get data from sql server table
  - Insert this data too into mysql database table

Read all data from mysql table and put into new csv file's
  - if data is more than 100000, distribute this data into multiple file
  - create short downloadable link 
 
 Fetch brand wise email ids from sql server db table
 
 send an Email with downloadable short links to brands.
