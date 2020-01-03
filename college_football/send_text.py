import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

email = ''
pas = ''

sms_gateway = '@txt.att.net'

smtp = 'smtp.gmail.com'
port = 587
server = smtplib.SMTP(smtp, port)

server.starttls()
server.login(email, pas)

msg = MIMEMultipart()
msg['From'] = email
msg['To'] = sms_gateway
msg['Subject'] = "\tsomething smells\n\n"
body = '\tgo change his diaper!\n'
msg.attach(MIMEText(body, 'plain'))

sms = msg.as_string()

server.sendmail(email, sms_gateway, sms)
server.quit()
